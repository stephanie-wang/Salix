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
    reply.Contents, reply.Err = kv.initReadBuffer(args.File, args.Bytes)
    reply.N, reply.Err = kv.readAt(args.File, reply.Contents, args.Offset, true)
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

func (kv *ShardKV) Remove(args *FileArgs, reply *Reply) error {
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

  var success bool
  switch op.Type {
  case Read, Write, Remove:
    success = kv.doFileOp(op)
  case Reconfig:
    success = kv.doReconfig(op)
    // for each config number, record the earliest successful paxos sequence
    // number, so we can clean up the tmp files once everyone is past this
    if _, inMap := kv.reconfigs[op.Num]; !inMap && success {
      kv.reconfigs[op.Num] = seq
    }
  case Reshard:
    success = kv.doReshard(op)
  }

  return success
}

func (kv *ShardKV) getMissingFiles(root string, files []string) []string {
  //TODO: also check if size is correct...
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()
  missing := []string{}
  for _, filename := range files {
    if !kv.exists(path.Join(root, filename)) {
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


func (kv *ShardKV) write(filename string, buf []byte, doAppend bool) (int, Err) {
  kv.fileMu.Lock()
  defer kv.fileMu.Unlock()

  shard := key2shard(filename)
  kv.popularityMu.Lock()
  kv.popularities[shard].writes++
  kv.popularityMu.Unlock()

  if !kv.exists(kv.getFilepath(filename)) {
    DPrintf("adding file %s to shard %s", filename, shard)
    kv.store[shard] = append(kv.store[shard], filename)
  }

  var f *os.File
  var err error
  defer f.Close()
  if doAppend {
    f, err = os.OpenFile(kv.getFilepath(filename), os.O_APPEND | os.O_CREATE, 0666)
  } else {
    f, err = os.Create(kv.getFilepath(filename))
  }
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

func (kv *ShardKV) doFileOp(op Op) bool {
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
    reply.Contents, reply.Err = kv.initReadBuffer(op.File, op.Bytes)
    reply.N, reply.Err = kv.readAt(op.File, reply.Contents, op.Offset, false)
  } else if op.Type == Write {
    val := op.Contents
    if op.DoHash {
      prevVal, _ := kv.initReadBuffer(op.File, -1)
      kv.readAt(op.File, prevVal, 0, false)
      reply.Contents = prevVal
      prevVal = append(prevVal, val...)
      hashed := hash(string(prevVal))
      val = []byte(strconv.Itoa(int(hashed)))
    }
    reply.N, reply.Err = kv.write(op.File, val, op.DoAppend)
  } else {
    err := os.Remove(kv.getFilepath(op.File))
    if err != nil {
      reply.Err = Err(err.Error())
    }
  }
  // save the reply
  kv.seen[shard][op.Id] = &reply
  return true
}

func (kv *ShardKV) doReconfig(op Op) bool {
  reconfig := op.ReconfigArgs
  if reconfig.Num <= kv.config.Num {
    // if we're already past this configuration, okay to return
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
  transferArgs := make(map[int64]*ReshardArgs)
  for shard, reconfigGid := range reconfig.Shards {
    if kv.config.Shards[shard] == kv.gid &&
      reconfig.Shards[shard] != kv.gid &&
      reconfig.Shards[shard] != 0 {
      // if shard belonged to us in old config and we have to transfer it to
      // a nonzero gid
      kv.initShardMap(shard)

      transferArg, inMap := transferArgs[reconfigGid]
      if !inMap {
        transferArg = &ReshardArgs{
          Num: reconfig.Num,
          Shards: make(map[int][]string),
          Seen: make(map[int]map[int64]*Reply),
        }
        transferArgs[reconfigGid] = transferArg
      }

      transferArg.Shards[shard] = kv.store[shard]
      transferArg.Seen[shard] = kv.seen[shard]

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
  // try to transfer all shards for one group at once
  for gid, transferArg := range transferArgs {
    go kv.transferShard(transferArg, reconfig.Groups[gid])
  }

  kv.config = *reconfig
  // record the paxos log seq for a successful reconfig
  DPrintf("new config on %d %d is %d", kv.me, kv.gid, kv.config.Num)
  return true
}

func (kv *ShardKV) doReshard(op Op) bool {
  shardReceived := true
  for shard, _ := range op.Shards {
    if op.Num > kv.shardConfigs[shard] {
      shardReceived = false
    }
    if op.Num != kv.shardConfigs[shard] + 1 {
      // if the shard we're receiving is more than one config away from the
      // current shard
      return false
    }
  }
  if shardReceived {
    return true
  }

  if op.Num != kv.config.Num {
    // if the shard we're receiving doesn't match the config we're trying to
    // reconfigure to
    return false
  }

  // at this point, must ensure that reshard fully executes

  tmpDir := kv.getTmpPathname(op.Num)
  // make array of missing files 
  // if any files missing, request until successful
  files := []string{}
  for _, shardFiles := range op.Shards {
    files = append(files, shardFiles...)
  }
  myAddr := kv.config.Groups[kv.gid][kv.me]
  args := &RequestFilesArgs{
    Address: myAddr,
    Files: kv.getMissingFiles(tmpDir, files),
    Num: op.Num,
  }
  DPrintf("resharding for config %d; need files on %d %d", args.Num, kv.me, kv.gid, args.Files)
  for server := 0; len(args.Files) > 0; server++ {
    serverAddr := op.ShardHolders[server % len(op.ShardHolders)]
    log.Println("requesting files from server %s", serverAddr)
    call(serverAddr, "ShardKV.RequestFiles", args, &Reply{})
    args.Files = kv.getMissingFiles(tmpDir, args.Files)
    log.Println("still missing files", args.Files)
  }

  isShardholder := false
  for _, holder := range op.ShardHolders {
    if myAddr == holder {
      isShardholder = true
    }
  }

  // if i'm a shardholder, copy files from tmp
  // else move files from tmp
  for shard, shardFiles := range op.Shards {
    for _, filename := range shardFiles {
      if isShardholder {
        tmp, err := os.Open(path.Join(tmpDir, filename))
        if err != nil {
          log.Println(err.Error())
        }
        f, err := os.Create(kv.getFilepath(filename))
        if err != nil {
          log.Println(err.Error())
        }
        io.Copy(f, tmp)
        tmp.Close()
        f.Close()
      } else {
        err := os.Rename(path.Join(tmpDir, filename), kv.getFilepath(filename))
        if err != nil {
          log.Println(err.Error())
        }
      }
    }
    kv.store[shard] = shardFiles
    kv.seen[shard] = op.Seen[shard]
    kv.shardConfigs[shard] = op.Num
  }

  // if ok to receive this shard, take filenames, take seen requests, update
  // which config this shard belongs to
  DPrintf("resharded woo for config %d on %d %d", args.Num, kv.me, kv.gid)

  return true
}


func (kv *ShardKV) initReadBuffer(base string, bytes int) ([]byte, Err) {
  filename := kv.getFilepath(base)
  var size int
  if !kv.exists(filename) {
    return []byte{}, "does not exist"
  }
  if bytes == -1 {
    f, err := os.Open(filename)
    if err != nil {
      return []byte{}, Err(err.Error())
    }
    fInfo, err := f.Stat()
    if err != nil {
      return []byte{}, Err(err.Error())
    }
    f.Close()
    size = int(fInfo.Size())
  } else {
    size = bytes
  }
  //reply.Contents = make([]byte, size)
  return make([]byte, size), ""
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
  attempted := false
  var server int
  var shardHolder int
  shardHolders := make(map[string]bool)
  for shardHolder < len(servers)/2 + 1 {
    serverAddr := servers[server % len(servers)]
    if _, inMap := shardHolders[serverAddr]; inMap {
      // skip servers that already have shard
      server++
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

    DPrintf("server %s is missing files", serverAddr, reply.Shard)

    if len(reply.Shard) == 0 {
      // if the server has all files in shard, record and move to next
      shardHolders[serverAddr] = true
      DPrintf("transfer from %d %d to %s was successful", kv.me, kv.gid, serverAddr)
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
      kv.sendFiles(serverAddr, filepaths, args.Num)
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
  server = 0
  kv.wait(func() bool {
    serverAddr := servers[server % len(servers)]
    var reply ReshardReply
    ok := call(serverAddr, "ShardKV.Reshard", args, &reply)
    if !ok || reply.Err == ErrWrongGroup {
      ok = false
    }
    server++
    return ok
  })

  // all files now transferred, so delete local copy of files
  // TODO: how are we going to do directory structure like this...
  for _, shardFiles := range args.Shards {
    for _, filename := range shardFiles {
      os.Remove(kv.getFilepath(filename))
    }
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
  for _, files := range args.Shards {
    missing := kv.getMissingFiles(kv.getTmpPathname(args.Num), files)
    reply.Shard = append(reply.Shard, missing...)
  }
  return nil
}

func (kv *ShardKV) sendFiles(dst string, files []*Filepath, config int) {
  dst = dst + "-net"
  for _, filepath := range files {
    conn, err := net.Dial("unix", dst)
    if err != nil {
      log.Println(err)
      continue
    }

    fi, _ := os.Stat(filepath.local)
    size := int(fi.Size())

    DPrintf("transferring file %s %s from %d %d to %s", filepath.local, filepath.base, kv.me, kv.gid, dst)
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

  if len(splitMeta) < 3 {
    DPrintf("did not receive all metadata on %d %d", kv.me, kv.gid)
    return
  }

  config, _ := strconv.Atoi(splitMeta[0])
  size, _ := strconv.Atoi(splitMeta[1])
  filename := strings.Join(splitMeta[2:], " ")

  if kv.config.Num != config {
    DPrintf("on %d not ready to receive file %s config %d on %d %d", kv.config.Num, filename, config, kv.me, kv.gid)
    return
  }

  log.Println("receiving file on", kv.me, kv.gid, config, filename)
  tmpPath := path.Join(kv.getTmpPathname(config), filename)

  // don't write the file again if it already exists and has correct size
  if kv.exists(tmpPath) {
    fi, _ := os.Stat(tmpPath)
    if int(fi.Size()) == size {
      log.Println("file already here")
      return
    }
  }

  f, err := os.Create(tmpPath)
  defer f.Close()
  if err != nil {
    log.Println(err.Error())
  }

  n, err := io.Copy(f, reader)

  if size != int(n) {
    // copy unsuccessful, so remove file while still under lock
    DPrintf("file transfer failed on", kv.me, kv.gid, config, filename)
    os.Remove(tmpPath)
  }
  if err != nil {
    log.Println(err.Error())
  }
}

func (kv *ShardKV) exists(filename string) bool {
  _, err := os.Stat(filename)
  return !(err != nil && os.IsNotExist(err))
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

  //if query.Num <= kv.config.Num {
  //  return
  //}

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
  DPrintf("decided op %d on %d %d", kv.seq, kv.me, kv.gid)

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
    DPrintf("cleaned %s", tmpDir)
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
  kv.reconfigs = make(map[int]int)

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
      if doLoadBalance {
        kv.popularityPing()
      }
      kv.cleanTmp()
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
      if !(err == nil && kv.dead == false) {
        return
      }
      if kv.unreliable && (rand.Int63() % 1000) < 100 {
        // discard the request.
        conn.Close()
      }
      //go func(c net.Conn) {
      //  kv.mu.Lock()
        kv.receiveFile(conn)
      //  kv.mu.Unlock()
      //}(conn)
    }
  }()

  return kv
}
