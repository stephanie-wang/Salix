package shardmaster

// 
// Shardmaster clerk.
// Because our failure model assumes that it is possible for a server
// to silently fail, lose its memory, and recover, it is necessary
// to attatch a random ID to every request the clerk sends.
// It is also necessary to add a new type of request (PopularityPing)
// for the automatic load balancing.
//
// These are the only changes made to this file.
//

import "net/rpc"
import "time"
import "fmt"

type Clerk struct {
  servers []string // shardmaster replicas
}

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
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

func (ck *Clerk) Query(num int) Config {
  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &QueryArgs{}
      args.Num = num
      args.ID = nrand()
      var reply QueryReply
      ok := call(srv, "ShardMaster.Query", args, &reply)
      if ok {
        return reply.Config
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
  return Config{}
}

func (ck *Clerk) Join(gid int64, servers []string) {
  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &JoinArgs{}
      args.GID = gid
      args.Servers = servers
      args.ID = nrand()
      var reply JoinReply
      ok := call(srv, "ShardMaster.Join", args, &reply)
      if ok {
        return
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (ck *Clerk) Leave(gid int64) {
  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &LeaveArgs{}
      args.GID = gid
      args.ID = nrand()
      var reply LeaveReply
      ok := call(srv, "ShardMaster.Leave", args, &reply)
      if ok {
        return
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (ck *Clerk) Move(shard int, gid int64) {
  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &MoveArgs{}
      args.Shard = shard
      args.GID = gid
      args.ID = nrand()
      var reply LeaveReply
      ok := call(srv, "ShardMaster.Move", args, &reply)
      if ok {
        return
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (ck *Clerk) PopularityPing(popularities map[int]int, config int, seq int, gid int64) {
  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &Popularity{
        Popularities: popularities,
        Seq: seq,
        Config: config,
        Gid: gid,
        ID: nrand(),
      }
      ok := call(srv, "ShardMaster.PopularityPing", args, args)
      if ok {
        return
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}
