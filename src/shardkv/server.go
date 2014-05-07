package shardkv

import "io"
import "net"
import "fmt"
import "net/rpc"
import "log"
import "path"
import "time"
import "paxos"
import "sync"
import "strconv"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug=1

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}

// TODO: need nrand id
type Op struct {
  // Your definitions here.
  Type string
  Id int64
  FileArgs
  ReconfigArgs *shardmaster.Config
  ReshardArgs
}

type ShardKV struct {
  mu sync.Mutex
  popularityMu sync.Mutex
  tickMu sync.Mutex
  l net.Listener
  fileL net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // next position to propose in paxos log
  seq int
  // current position in paxos log
  current int
  // highest config number successfully queried during a tick so far
  lastConfig int
  // current configuration
  config shardmaster.Config
  // store of shard number -> list of filenames in that shard
  store map[int][]string
  // a map of shard number -> map of req id -> reply, for seen requests
  seen map[int]map[int64]*Reply
  // a map of shard number -> config number. Tell us which config the current
  // shard we have is associated with
  shardConfigs map[int]int

  // popularity scores
  popularities map[int]*PopularityStatus

  root string
}


func (kv *ShardKV) Read(args *FileArgs, reply *Reply) error {
  op := Op{
    Type: Read,
    Id: args.Id,
    FileArgs: *args,
  }

  if reply.Err != "" {
    return nil
  }

  // stale reads are not proposed to paxos log
  if args.Stale {
    initReadBuffer(args.File, args.Bytes, reply)
    reply.N, reply.Err = kv.readAt(args.File, reply.Contents, args.Off, true)
    return nil
  }

  success := kv.proposeOp(op)
  if success {
    seenReply := kv.seen[key2shard(args.File)][args.Id]
    reply.Err, reply.N, reply.Contents = seenReply.Err, seenReply.N, seenReply.Contents
  } else {
    reply.Err = ErrWrongGroup
  }
  return nil
}

func (kv *ShardKV) Write(args *FileArgs, reply *Reply) error {
  op := Op{
    Type: Write,
    Id: args.Id,
    FileArgs: *args,
  }
  success := kv.proposeOp(op)
  if success {
    seenReply := kv.seen[key2shard(args.File)][args.Id]
    reply.Err, reply.N, reply.Contents = seenReply.Err, seenReply.N, seenReply.Contents
  } else {
    reply.Err = ErrWrongGroup
  }
  return nil
}


func (kv *ShardKV) doOp(seq int) bool {
  /*
  Waits until op with seqence # seq in the log is decided, then performs
  Returns true for success
  Ops are:
    -get/put (like kvpaxos)
    -reconfig - changing to a new config
    -reshard - receiving a shard from another server
    -nop - do nothing
  */

  decided, _ := kv.px.Status(seq)
  // propose a nop to force a decision if this op isn't decided yet
  //  then wait for paxos to decide before executing op
  if !decided {
    nop := Op{Type: Nop}
    kv.px.Start(seq, nop)
    kv.wait(func() bool {
      decided, _ := kv.px.Status(seq)
      return decided
    })
  }

  _, val := kv.px.Status(seq)
  op, _ := val.(Op)

  DPrintf("executing op %s seq %d on server %d %d", op.Type, seq, kv.me, kv.gid)

  switch op.Type {
  case Read, Write:
    shard := key2shard(op.File)
    if !(kv.config.Shards[shard] == kv.gid && kv.shardConfigs[shard] == kv.config.Num) {
      // if shard doesn't belong to us or we haven't received that shard yet
      return false
    }

    if _, seen := kv.seen[shard][op.Id]; seen {
      // if we've already seen this
      return true
    }

    kv.initShardMap(shard)
    var reply Reply
    if op.Type == Read {
      initReadBuffer(op.File, op.Bytes, &reply)
      reply.N, reply.Err = kv.readAt(op.File, reply.Contents, op.Off, false)
    } else {
      // TODO: keep dohash for now so we can test
      //val := ""
      //if op.DoHash {
      //  // TODO: read file instead of map
      //  //prevVal, _ := kv.store[shard][op.Key]
      //  prevVal := ""
      //  val = strconv.Itoa(int(hash(prevVal + val)))
      //  reply = Reply{Value: prevVal}
      //}
      reply.N, reply.Err = kv.write(op.File, op.Contents)
    }
    // save the reply
    kv.seen[shard][op.Id] = &reply

  case Reconfig:
    reconfig := op.ReconfigArgs
    DPrintf("executing reconfig %d on %d %d", reconfig.Num, kv.me, kv.gid)
    if reconfig.Num <= kv.config.Num {
      // if we've already past this configuration, okay to return
      DPrintf("already past config %d on %d %d", reconfig.Num, kv.me, kv.gid)
      return true
    }
    if reconfig.Num != kv.config.Num + 1 {
      // reconfig is too high, we're not ready for this reconfig yet
      DPrintf("at config %d but reconfig to %d", kv.config.Num, reconfig.Num)
      return false
    }

    // all shards must be up to date with current config before we move on to
    // the next config
    for shard, shardConfigNum := range kv.shardConfigs {
      if shardConfigNum != kv.config.Num {
        DPrintf("shard %d at %d not config %d on %d %d", shard, shardConfigNum, kv.config.Num, kv.me, kv.gid)
        return false
      }
    }

    DPrintf("attempt reconfigure to", reconfig)
    kv.popularityMu.Lock()
    defer kv.popularityMu.Unlock()
    for shard, reconfigGid := range reconfig.Shards {
      if kv.config.Shards[shard] == kv.gid &&
        reconfig.Shards[shard] != kv.gid &&
        reconfig.Shards[shard] != 0 {
        // if shard belonged to us in old config and we have to transfer it to
        // a nonzero gid
        // TODO: transfer all files
        kv.initShardMap(shard)

        //shardFiles := make([]string, len(kv.store[shard]))
        //copy(shardFiles, kv.store[shard])
        //shardSeen := make(map[int64]*Reply)
        //for id, reply := range kv.seen[shard] {
        //  shardSeen[id] = reply
        //}

        args := &ReshardArgs{
          Num: reconfig.Num,
          ShardNum: shard,
          //Shard: shardFiles,
          //Seen: shardSeen,
          Shard: kv.store[shard],
          Seen: kv.seen[shard],
        }
        go kv.transferShard(args, reconfig.Groups[reconfigGid])
      }

      if !(kv.config.Shards[shard] != kv.gid &&
        reconfig.Shards[shard] == kv.gid &&
        kv.config.Shards[shard] != 0) {
        // if it's not a shard we expect to receive from another gid, then this
        // shard is up to date with the new config
        kv.shardConfigs[shard] = reconfig.Num
      }

      // reset all popularities after a reconfig
      kv.popularities[shard] = &PopularityStatus{}
    }

    kv.config = *reconfig
    DPrintf("new config on %d %d is %d", kv.me, kv.gid, kv.config.Num)

  case Reshard:
    DPrintf("reshard %d for config %d while on config %d on %d %d", op.ShardNum, op.Num, kv.config.Num, kv.me, kv.gid)

    if op.Num <= kv.shardConfigs[op.ShardNum] {
      // if we've already received the shard for this config number
      return true
    }
    if op.Num != kv.shardConfigs[op.ShardNum] + 1 {
      // if the shard we're receiving is more than one config away from the
      // current shard
      return false
    }

    if op.Num != kv.config.Num {
      // if the shard we're receiving doesn't match the config we're trying to
      // reconfigure to
      return false
    }

    tmpDir := kv.getTmpConfigDir(op.Num)

    // make array of missing files 
    // if any files missing, request until successful
    // NOTE: cannot return false in this case because group will be out of sync
    args := &RequestFilesArgs{
      Address: kv.config.Groups[kv.gid][kv.me],
      Files: kv.getMissingFiles(tmpDir, op.Shard),
    }
    for server := 0; len(args.Files) > 0; server++ {
      // TODO: don't join with tmpdir on the receiving end 
      serverAddr := op.ShardHolders[server % len(op.ShardHolders)]
      call(serverAddr, "ShardKV.RequestFiles", args, &Reply{})
      args.Files = kv.getMissingFiles(tmpDir, args.Files)
    }

    //copy over all files from tmp 
    for _, filename := range op.Shard {
      tmp, err := os.Open(path.Join(tmpDir, filename))
      if err != nil {
        // TODO: what to do in this case?
        log.Println(err.Error())
      }
      f, err := os.Create(filename)
      if err != nil {
        // TODO: what to do in this case?
        log.Println(err.Error())
      }
      io.Copy(f, tmp)
      tmp.Close()
      f.Close()
    }

    // if ok to receive this shard, take key-values, take seen requests, update
    // which config this shard belongs to
    kv.store[op.ShardNum] = op.Shard
    kv.seen[op.ShardNum] = op.Seen
    kv.shardConfigs[op.ShardNum] = op.Num
  }

  return true
}

func (kv *ShardKV) getMissingFiles(root string, files []string) []string {
  missing := []string{}
  for _, filename := range files {
    _, err := os.Stat(path.Join(root, filename))
    if err != nil && os.IsNotExist(err) {
      missing = append(missing, filename)
    }
  }
  return missing
}

func (kv *ShardKV) readAt(filename string, buf []byte, off int64, stale bool) (int, Err) {
  shard := key2shard(filename)
  kv.popularityMu.Lock()
  if stale {
    kv.popularities[shard].staleReads++
  } else {
    kv.popularities[shard].reads++
  }
  kv.popularityMu.Unlock()

  f, err := os.Open(filename)
  defer f.Close()
  if err != nil {
    return 0, Err(err.Error())
  }

  n, err := f.ReadAt(buf, off)
  if err != nil {
    return n, Err(err.Error())
  }
  return n, ""
}


func (kv *ShardKV) write(filename string, buf []byte) (int, Err) {
  shard := key2shard(filename)
  kv.popularityMu.Lock()
  kv.popularities[shard].writes++
  kv.popularityMu.Unlock()
  log.Println(os.Getwd())

  _, err := os.Stat(filename)
  if err != nil && os.IsNotExist(err) {
    log.Printf("adding file %s to shard %s", filename, shard)
    kv.store[shard] = append(kv.store[shard], filename)
  }

  f, err := os.Create(filename)
  defer f.Close()
  if err != nil {
    return 0, Err(err.Error())
  }

  n, err := f.Write(buf)
  if err != nil {
    return n, Err(err.Error())
  }

  f2, _ := os.Open(filename)
  f2.Read(buf)
  return n, ""
}

func initReadBuffer(filename string, bytes int, reply *Reply) {
  var size int
  if bytes == -1 {
    f, err := os.Open(filename)
    if err != nil {
      reply.Err = Err(err.Error())
      return
    }
    fInfo, err := f.Stat()
    if err != nil {
      reply.Err = Err(err.Error())
      return
    }
    f.Close()
    size = int(fInfo.Size())
  } else {
    size = bytes
  }
  reply.Contents = make([]byte, size)
}

func (kv *ShardKV) initShardMap(shard int) {
  /*
  Start up a blank map for a shard if it doesn't exist yet, in the key-value
  store and the mapping of shard -> seen requests
  */
  if _, inMap := kv.store[shard]; !inMap {
    kv.store[shard] = []string{}
  }
  if _, inMap := kv.seen[shard]; !inMap {
    kv.seen[shard] = make(map[int64]*Reply)
  }
}

func (kv *ShardKV) getTmpConfigDir(configNum int) string {
  tmpDir := path.Join(kv.root, "tmp", strconv.Itoa(configNum))
  err := os.MkdirAll(tmpDir, 0777)
  if err != nil {
    log.Println(err.Error())
  }
  return tmpDir
}

func (kv *ShardKV) transferShard(args *ReshardArgs, servers []string) {
  /*
  Try to transfer a shard to one of the servers until that server responds ok
  Receiving server might not be in the right group yet, according to sender's
  config, so will send back ErrWrongGroup if it's not ready for this shard yet
  */

  // keep transferring until a majority of servers have files they need
  // TODO: change this later so that we set a minimum
  var server int
  attempted := false
  args.ShardHolders = make([]string, len(servers)/2 + 1)
  var shardHolder int
  for shardHolder < len(args.ShardHolders) {
    serverAddr := servers[server % len(servers)] + "-net"
    reply := &ReshardReply{
      Shard: []string{},
    }
    ok := call(serverAddr, "ShardKV.ReceiveShard", args, &reply)
    // server unreachable?
    if !ok {
      server++
      continue
    }

    if len(reply.Shard) == 0 {
      // if the server has all files in shard
      args.ShardHolders[shardHolder] = serverAddr
      server++
      attempted = false
    } else {
      // server still missing some shards
      if attempted {
        // if we have already attempted this server, file transfer must have
        // failed - try next server
        server++
        attempted = false
      } else {
        // if we haven't attempted this server once yet, transfer files and try again
        kv.sendFiles(serverAddr, reply.Shard, args.Num)
        attempted = true
      }
    }
  }

  // propose reshard until successful
  DPrintf("shard %d has files", args.ShardNum, args.Shard)
  server = 0
  for ok := false; !ok; {
    serverAddr := servers[server % len(servers)]
    DPrintf("transferring shard %d for config %d from %d %d to %d",
      args.ShardNum,
      args.Num,
      kv.me,
      kv.gid,
      server % len(servers))
    var reply ReshardReply
    ok = call(serverAddr, "ShardKV.Reshard", args, &reply)
    if !ok || reply.Err == ErrWrongGroup {
      ok = false
    }
    server++
  }

  // TODO: all files now transferred, so delete local copy of files
  //for _, filename := range args.Shard {
  //  os.Remove(filename)
  //}

}

func (kv *ShardKV) RequestFiles(args *RequestFilesArgs, reply *Reply) error {
  tmpDir := kv.getTmpConfigDir(args.Num)
  tmpFiles := make([]string, len(args.Files))
  for i, filename := range args.Files {
    tmpFiles[i] = path.Join(tmpDir, filename)
  }
  kv.sendFiles(args.Address + "-net", tmpFiles, args.Num)
  return nil
}

func (kv *ShardKV) sendFiles(dst string, files []string, config int) {
  conn, err := net.Dial("unix", dst)
  if err != nil {
    log.Println(err)
  }
  defer conn.Close()
  encoder := gob.NewEncoder(conn)
  log.Printf("transferring files from %d %d", kv.me, kv.gid, files)
  for _, filename := range files {
    log.Printf("writing file %s to conn", filename)

    meta := &FileMetadata{
      Filename: filename,
      Num: config,
    }
    encoder.Encode(meta)

    f, err := os.Open(filename)
    defer f.Close()
    if err != nil {
      log.Println(err)
    }
    _, err = io.Copy(conn, f)
    if err != nil {
      log.Println(err)
    }
  }
}

func (kv *ShardKV) ReceiveShard(args *ReshardArgs, reply *ReshardReply) error {
  tmpDir := kv.getTmpConfigDir(args.Num)
  reply.Shard = []string{}
  for _, filename := range args.Shard {
    _, err := os.Stat(path.Join(tmpDir, filename))
    if err != nil && os.IsNotExist(err) {
      reply.Shard = append(reply.Shard, filename)
    }
  }
  return nil
}

func (kv *ShardKV) wait(condition func() bool) {
  /*
  Wait for increasingly long intervals until the condition() function is
  satisfied
  */
  to := 10 * time.Millisecond
  for {
    if condition() {
      return
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  /*
  Ping shardmaster for the next reconfiguration expected. Start up a proposer
  thread if the next configuration is returned
  */
  kv.tickMu.Lock()
  defer kv.tickMu.Unlock()

  query := kv.sm.Query(kv.lastConfig + 1)
  if query.Num != kv.lastConfig + 1 {
    return
  }

  go func() {
    op := Op{
      Type: Reconfig,
      ReconfigArgs: &query,
      Id: nrand(),
    }
    // wait until the proposal is successful
    kv.wait(func() bool {
      return kv.proposeOp(op)
    })
  }()
  kv.lastConfig = query.Num
}

func (kv *ShardKV) popularityPing() {
  kv.popularityMu.Lock()
  defer kv.popularityMu.Unlock()
  // don't allow reconfigs during a ping
  // is this safe from deadlock?
  kv.mu.Lock()
  defer kv.mu.Unlock()

  popularities := make(map[int]int)
  for shard, gid := range kv.config.Shards {
    if gid == kv.gid {
      popularities[shard] = kv.popularities[shard].popularity()
    }
  }
  kv.sm.PopularityPing(popularities, kv.config.Num, kv.gid)
}

func (kv *ShardKV) Reshard(args *ReshardArgs, reply *Reply) error {
  /* 
  propose reshard (receive a shard) op. Set ErrWrongGroup if wasn't
  successfully executed
  */
  op := Op{
    Type: Reshard,
    ReshardArgs: *args,
    Id: nrand(),
  }
  ok := kv.proposeOp(op)
  if !ok {
    reply.Err = ErrWrongGroup
  }

  return nil
}

func (kv *ShardKV) proposeOp(op Op) bool {
  /*
  Propose the given op and execute all ops up to and including it until the op
  is successfully executed
  Return success
  */
  kv.mu.Lock()
  defer kv.mu.Unlock()
  for proposed := false; !proposed; {
    // propose the op until the op decided by paxos matches ours
    // lock to prevent other ops from being proposed w/same seq
    kv.px.Start(kv.seq, op)
    kv.wait(func() bool {
      decided, _ := kv.px.Status(kv.seq)
      return decided
    })
    _, val := kv.px.Status(kv.seq)
    decidedOp, ok := val.(Op)
    if !ok {
      proposed = false
    } else {
      proposed = op.Id == decidedOp.Id
    }
    kv.seq++
  }

  // execute up to the op and check if successful
  var success bool
  for kv.current < kv.seq {
    success = kv.doOp(kv.current)
    kv.current++
  }
  //finished interpreting up until current so ok to discard
  kv.px.Done(kv.current - 1)
  return success
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  //kv.fileL.Close()
  kv.px.Kill()
}

func (kv *ShardKV) receiveFiles(conn net.Conn) {
  for {
    dec := gob.NewDecoder(conn)
    meta := &FileMetadata{}
    if err := dec.Decode(meta); err == io.EOF {
      conn.Close()
      break
    }

    log.Printf("copying file %s config %d", meta.Filename, meta.Num)
    tmpDir := kv.getTmpConfigDir(meta.Num)
    f, err := os.Create(path.Join(tmpDir, meta.Filename))
    defer f.Close()
    if err != nil {
      log.Println(err.Error())
    }

    io.Copy(f, conn)
    log.Println("hey")
  }
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  // start off asking for config 0, so set lastConfig seen to -1
  kv.lastConfig = -1
  // mapping of shard num -> list of filenames in that shard
  kv.store = make(map[int][]string)
  kv.seen = make(map[int]map[int64]*Reply)
  kv.shardConfigs = make(map[int]int)
  kv.popularities = make(map[int]*PopularityStatus)

  // TODO: consider chrooting to this
  kv.root = servers[me] + "-root"
  os.MkdirAll(path.Join(kv.root, "tmp"), 0777)
  dir, _ := os.Open(kv.root)
  dir.Chdir()

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      kv.popularityPing()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  os.Remove(servers[me] + "-net")
  ln, err := net.Listen("unix", servers[me] + "-net")
  if err != nil {
    log.Println(err.Error())
  }
  kv.fileL = ln
  go func() {
    for kv.dead == false {
      conn, err := kv.fileL.Accept()
      if err != nil {
        log.Println(err.Error())
      }
      go kv.receiveFiles(conn)
    }
  }()

  return kv
}
