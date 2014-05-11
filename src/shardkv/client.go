package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  sm *shardmaster.Clerk
  config shardmaster.Config
  // You'll have to modify Clerk.
  // TODO: consider giving clients unique id instead of reqs...
}



func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  // You'll have to modify MakeClerk.
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  // TODO: think hard about this guy...current mapping won't work for files 
  // TODO: make sure that a lock file gets mapped to the same shard as the file it locks
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Read(file string, bytes int, offset int64, stale bool) []byte {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Read().
  args := &FileArgs{
    File: file,
    Id: nrand(),
    Bytes: bytes,
    Offset: offset,
    Stale: stale,
  }

  for {
    shard := key2shard(file)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        var reply Reply
        reply.Err = OK
        ok := call(srv, "ShardKV.Read", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          return reply.Contents
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
  return []byte{}
}

func (ck *Clerk) WriteExt(file string, contents []byte, doAppend bool, doHash bool) []byte {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  args := &FileArgs{
    File: file,
    Contents: contents,
    DoHash: doHash,
    DoAppend: doAppend,
    Id: nrand(),
  }

  for {
    shard := key2shard(file)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        var reply Reply
        reply.Err = OK
        ok := call(srv, "ShardKV.Write", args, &reply)
        if ok && reply.Err == OK {
          return reply.Contents
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Write(file string, contents []byte, doAppend bool) {
  ck.WriteExt(file, contents, doAppend, false)
}
func (ck *Clerk) WriteHash(file string, contents[]byte, doAppend bool) []byte {
  v := ck.WriteExt(file, contents, doAppend, true)
  return v
}

func (ck *Clerk) Remove(file string) {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Read().
  args := &FileArgs{
    File: file,
    Id: nrand(),
  }

  for {
    shard := key2shard(file)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        var reply Reply
        reply.Err = OK
        ok := call(srv, "ShardKV.Remove", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          return
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}
