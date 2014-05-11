package shardkv

import "bufio"
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
import "strings"
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
  fileMu sync.Mutex
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

  // map of config number -> paxos seq of corresponding reconfig op
  reconfigs map[int]int
  cleanedConfig int
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
    if kv.config.Shards[key2shard(args.File)] != kv.gid {
      reply.Err = ErrWrongGroup
      return nil
    }
    initReadBuffer(kv.getFilepath(args.File), args.Bytes, reply)
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

  log.Printf("executing op %s seq %d on server %d %d", op.Type, seq, kv.me, kv.gid)

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
      initReadBuffer(kv.getFilepath(op.File), op.Bytes, &reply)
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
    if reconfig.Num <= kv.config.Num {
      // if we've already past this configuration, okay to return
      log.Printf("already past config %d on %d %d", reconfig.Num, kv.me, kv.gid)
      return true
    }
    if reconfig.Num != kv.config.Num + 1 {
      // reconfig is too high, we're not ready for this reconfig yet
      log.Printf("at config %d but reconfig to %d", kv.config.Num, reconfig.Num)
      return false
    }

    // all shards must be up to date with current config before we move on to
    // the next config
    for shard, shardConfigNum := range kv.shardConfigs {
      if shardConfigNum != kv.config.Num {
        log.Printf("shard %d at %d not config %d on %d %d", shard, shardConfigNum, kv.config.Num, kv.me, kv.gid)
        return false
      }
    }

    log.Printf("attempt reconfigure to", reconfig)
    kv.popularityMu.Lock()
    defer kv.popularityMu.Unlock()
    for shard, reconfigGid := range reconfig.Shards {
      if kv.config.Shards[shard] == kv.gid &&
        reconfig.Shards[shard] != kv.gid &&
        reconfig.Shards[shard] != 0 {
        // if shard belonged to us in old config and we have to transfer it to
        // a nonzero gid
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
      // kv.popularities[shard] = &PopularityStatus{}
    }

    kv.config = *reconfig
    // record the paxos log seq for a successful reconfig
    kv.reconfigs[op.Num] = seq
    log.Printf("new config on %d %d is %d", kv.me, kv.gid, kv.config.Num)

  case Reshard:
    //log.Printf("reshard %d for config %d while on config %d on %d %d", op.ShardNum, op.Num, kv.config.Num, kv.me, kv.gid)

    if op.Num <= kv.shardConfigs[op.ShardNum] {
      // if we've already received the shard for this config number
      log.Printf("no reshard %d on %d %d", op.ShardNum, kv.me, kv.gid)
      return true
    }
    if op.Num != kv.shardConfigs[op.ShardNum] + 1 {
      // if the shard we're receiving is more than one config away from the
      // current shard
      log.Printf("no reshard %d on %d %d", op.ShardNum, kv.me, kv.gid)
      return false
    }

    if op.Num != kv.config.Num {
      // if the shard we're receiving doesn't match the config we're trying to
      // reconfigure to
      log.Printf("no reshard %d on %d %d", op.ShardNum, kv.me, kv.gid)
      return false
    }

    tmpDir := kv.getTmpPathname(op.Num)

    // make array of missing files 
    // if any files missing, request until successful
    // NOTE: cannot return false in this case because group will be out of sync
    args := &RequestFilesArgs{
      Address: kv.config.Groups[kv.gid][kv.me] + "-net",
      Files: kv.getMissingFiles(tmpDir, op.Shard),
      Num: op.Num,
    }
    log.Printf("resharding for config %d; need files on %d %d", args.Num, kv.me, kv.gid, args.Files)
    for server := 0; len(args.Files) > 0; server++ {
      serverAddr := op.ShardHolders[server % len(op.ShardHolders)]
      log.Println("requesting files from server %s", serverAddr)
      call(serverAddr, "ShardKV.RequestFiles", args, &Reply{})
      args.Files = kv.getMissingFiles(tmpDir, args.Files)
      log.Println("still missing files", args.Files)
    }

    //copy over all files from tmp 
    for _, filename := range op.Shard {
      tmp, err := os.Open(path.Join(tmpDir, filename))
      if err != nil {
        // TODO: what to do in this case?
        log.Println(err.Error())
      }
      f, err := os.Create(kv.getFilepath(filename))
      if err != nil {
        // TODO: what to do in this case?
        log.Println(err.Error())
      }
      io.Copy(f, tmp)
      tmp.Close()
      f.Close()
    }
    // TODO: if we're not in the list of shardholders, okay to delete tmp file now!

    // if ok to receive this shard, take filenames, take seen requests, update
    // which config this shard belongs to
    kv.store[op.ShardNum] = op.Shard
    kv.seen[op.ShardNum] = op.Seen
    kv.shardConfigs[op.ShardNum] = op.Num
    log.Printf("resharded woo for config %d on %d %d", args.Num, kv.me, kv.gid)

  }

  return true
}

func (kv *ShardKV) getMissingFiles(root string, files []string) []string {
  log.Printf("1 got here on %d %d", kv.me, kv.gid)
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()
  log.Printf("2 got here on %d %d", kv.me, kv.gid)
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
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()

  shard := key2shard(filename)
  kv.popularityMu.Lock()
  if stale {
    kv.popularities[shard].staleReads++
  } else {
    kv.popularities[shard].reads++
  }
  kv.popularityMu.Unlock()

  f, err := os.Open(kv.getFilepath(filename))
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
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()

  shard := key2shard(filename)
  kv.popularityMu.Lock()
  kv.popularities[shard].writes++
  kv.popularityMu.Unlock()

  _, err := os.Stat(kv.getFilepath(filename))
  if err != nil && os.IsNotExist(err) {
    log.Printf("adding file %s to shard %s", filename, shard)
    kv.store[shard] = append(kv.store[shard], filename)
  }

  f, err := os.Create(kv.getFilepath(filename))

  defer f.Close()
  if err != nil {
    return 0, Err(err.Error())
  }

  n, err := f.Write(buf)
  if err != nil {
    return n, Err(err.Error())
  }

  f2, _ := os.Open(kv.getFilepath(filename))
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

func (kv *ShardKV) getFilepath(filepath string) string {
  return path.Join(kv.root, filepath)
}

func (kv *ShardKV) getTmpPathname(configNum int) string {
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
  attempted := false
  var server int
  var shardHolder int
  shardHolders := make(map[string]bool)
  for shardHolder < len(servers)/2 + 1 {
    serverAddr := servers[server % len(servers)]
    if _, ok := shardHolders[serverAddr]; ok {
      // skip servers that already have shard
      continue
    }
    reply := &ReshardReply{
      Shard: []string{},
    }
    ok := call(serverAddr, "ShardKV.MissingFiles", args, &reply)
    // server unreachable?
    if !ok {
      server++
      continue
    }

    log.Printf("server %s is missing files", serverAddr, reply.Shard)

    if len(reply.Shard) == 0 {
      // if the server has all files in shard, record and move to next
      shardHolders[serverAddr] = true
      log.Printf("transfer from %d %d to %s was successful", kv.me, kv.gid, serverAddr)
      shardHolder++
      server++
      attempted = false
      continue
    }

    // server still missing some shards
    if attempted {
      // if we have already attempted this server, file transfer must have
      // failed - try next server
      server++
      attempted = false
    } else {
      // if we haven't attempted this server once yet, transfer files and try again
      filepaths := make([]*Filepath, len(reply.Shard))
      for i, filename := range reply.Shard {
        filepaths[i] = &Filepath{
          local: kv.getFilepath(filename),
          base: filename,
        }
      }
      //kv.sendFiles(serverAddr + "-net", reply.Shard, args.Num)
      kv.sendFiles(serverAddr + "-net", filepaths, args.Num)
      attempted = true
    }
  }

  args.ShardHolders = make([]string, len(servers)/2 + 1)
  var i int
  for serverAddr, _ := range shardHolders {
    args.ShardHolders[i] = serverAddr
    i++
  }

  // propose reshard until successful
  log.Printf("shard %d has files", args.ShardNum, args.Shard)
  server = 0
  for ok := false; !ok; {
    serverAddr := servers[server % len(servers)]
    log.Printf("proposing reshard %d for config %d from %d %d to %d",
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

  // all files now transferred, so delete local copy of files
  // TODO: how are we going to do directory structure like this...
  for _, filename := range args.Shard {
    os.Remove(kv.getFilepath(filename))
  }

}

func (kv *ShardKV) RequestFiles(args *RequestFilesArgs, reply *Reply) error {
  tmpDir := kv.getTmpPathname(args.Num)
  filepaths := make([]*Filepath, len(args.Files))
  for i, filename := range args.Files {
    filepaths[i] = &Filepath{
      local: path.Join(tmpDir, filename),
      base: filename,
    }
  }
  kv.sendFiles(args.Address, filepaths, args.Num)
  return nil
}

func (kv *ShardKV) MissingFiles(args *ReshardArgs, reply *ReshardReply) error {
  reply.Shard = kv.getMissingFiles(kv.getTmpPathname(args.Num), args.Shard)
  return nil
}

func (kv *ShardKV) sendFiles(dst string, files []*Filepath, config int) {
  // NOTE: not sure if fileMu.lock is needed here...don't think so
  for _, filepath := range files {
    conn, err := net.Dial("unix", dst)
    if err != nil {
      log.Println(err)
      continue
    }

    fi, _ := os.Stat(filepath.local)
    size := int(fi.Size())

    log.Printf("transferring file %s %s from %d %d to %s", filepath.local, filepath.base, kv.me, kv.gid, dst)
    meta := []string{strconv.Itoa(config), strconv.Itoa(size), filepath.base}
    metaString := strings.Join(meta, " ") + "\n"
    conn.Write([]byte(metaString))

    f, err := os.Open(filepath.local)
    defer f.Close()
    if err != nil {
      log.Println(err)
    }
    _, err = io.Copy(conn, f)
    if err != nil {
      log.Println(err)
    }

    // NOTE: is it okay to keep the connection open on this end?
    //   not sure if keeping it open lowers file transfer failure rate...?
    conn.Close()
  }
}

func (kv *ShardKV) receiveFile(conn net.Conn) {
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()

  defer conn.Close()
  reader := bufio.NewReader(conn)
  meta, _ := reader.ReadString('\n')

  splitMeta := strings.Fields(meta)
  config, _ := strconv.Atoi(splitMeta[0])
  size, _ := strconv.Atoi(splitMeta[1])
  filename := strings.Join(splitMeta[2:], " ")

  log.Println("receiving file on", kv.me, kv.gid, config, filename)
  tmpDir := kv.getTmpPathname(config)

  // don't write the file again if it already exists and has correct size
  fi, err := os.Stat(path.Join(tmpDir, filename))
  if !(err != nil && os.IsNotExist(err)) && int(fi.Size()) == size {
    log.Println("file already here")
    return
  }

  f, err := os.Create(path.Join(tmpDir, filename))
  defer f.Close()
  if err != nil {
    log.Println(err.Error())
  }

  //w := io.MultiWriter(os.Stdout, f)
  //n, err := io.Copy(w, reader)
  n, err := io.Copy(f, reader)

  if size != int(n) {
    // copy unsuccessful, so remove file while still under lock
    log.Println("file transfer failed on", kv.me, kv.gid, config, filename)
    os.Remove(path.Join(tmpDir, filename))
  }
  if err != nil {
    log.Println(err.Error())
  }
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

  kv.lastConfig = query.Num
  if query.Num <= kv.config.Num {
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
}

func (kv *ShardKV) popularityPing() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.popularityMu.Lock()
  defer kv.popularityMu.Unlock()
  // don't allow reconfigs during a ping
  // is this safe from deadlock?

  popularities := make(map[int]int)
  for shard, gid := range kv.config.Shards {
    if gid == kv.gid {
      popularities[shard] = kv.popularities[shard].popularity()
    }
  }
  kv.sm.PopularityPing(popularities, kv.config.Num, kv.current, kv.gid)
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
  log.Printf("received reshard %d op %d %d", op.ShardNum, kv.me, kv.gid)
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
  log.Printf("1 got here proposing on %d %d", kv.me, kv.gid)
  kv.mu.Lock()
  defer kv.mu.Unlock()
  log.Printf("2 got here proposing on %d %d", kv.me, kv.gid)
  for proposed := false; !proposed; {
    // propose the op until the op decided by paxos matches ours
    // lock to prevent other ops from being proposed w/same seq
    if op.Type == Reshard {
      log.Printf("proposing reshard %d for seq %d on %d %d", op.ShardNum, kv.seq, kv.me, kv.gid)
    }
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

func (kv *ShardKV) cleanTmp() {
  /*
  Try to clean up tmp files for the most recent reconfig
  */
  min := kv.px.Min()

  if kv.cleanedConfig > kv.config.Num {
    // we haven't reached the next config yet
    return
  }

  for kv.cleanedConfig <= kv.config.Num {
    if seq, ok := kv.reconfigs[kv.cleanedConfig]; ok && seq >= min {
      // we have reached next config, but not all instances have applied it yet
      return
    }
    // either:
    //  - next config had no sequence in the log --> this instance is starting
    //  at this config 
    //  - next config is in log and all instances have applied 
    // then it's okay to delete the tmp files for the previous config
    tmpDir := kv.getTmpPathname(kv.cleanedConfig - 1)
    log.Println(tmpDir)
    err := os.RemoveAll(tmpDir)
    if err != nil {
      log.Println(err)
    }
    log.Printf("cleaned %s", tmpDir)
    kv.cleanedConfig++
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
                 servers []string, me int, doLoadBalance bool) *ShardKV {
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
  for i := 0; i < shardmaster.NShards; i++ {
    kv.popularities[i] = &PopularityStatus{}
  }
  kv.reconfigs = make(map[int]int)

  // TODO: consider chrooting to this
  kv.root = servers[me] + "-root"
  os.MkdirAll(path.Join(kv.root, "tmp"), 0777)

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
      kv.cleanTmp()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  go func() {
    for kv.dead == false {
      if doLoadBalance {
        kv.popularityPing()
      }
      time.Sleep(5000 * time.Millisecond)
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
      if !(err == nil && kv.dead == false) {
        return
      }
      if kv.unreliable && (rand.Int63() % 1000) < 100 {
        // discard the request.
        conn.Close()
      }
      go kv.receiveFile(conn)
    }
  }()

  return kv
}
