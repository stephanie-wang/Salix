package shardkv

import "shardmaster"
import "hash/fnv"
import "crypto/rand"
import "math/big"

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

type PopularityStatus struct {
  reads int
  staleReads int
  writes int
}
func (ps *PopularityStatus) popularity() int {
  return 1 + ps.reads + ps.staleReads + ps.writes
}

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  Read = "Read"
  Write = "Write"
  Reconfig = "Reconfig"
  Reshard = "Reshard"
  Nop = "Nop"
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type FileArgs struct {
  File string
  Contents []byte
  Id int64
  Bytes int
  Off int64
  Stale bool // for read only
  DoHash bool // for write only
}

type Filepath struct {
  local string
  base string
}

//type WriteArgs struct {
//  File string
//  Value string
//  DoHash bool  // For PutHash
//  // You'll have to add definitions here.
//  // Field names must start with capital letters,
//  // otherwise RPC will break.
//  Id int64
//  Bytes int
//  Off int64
//}

//type PutReply struct {
  //Err Err
  //PreviousValue string   // For PutHash
//}

//type ReadArgs struct {
//  File string
//  Id int64
//  Stale bool
//  Bytes int
//  Off int64
//}

//type GetReply struct {
type Reply struct {
  Err Err
  N int // number of bytes successfully read/written
  Contents []byte
}

type ReconfigArgs struct {
  Config shardmaster.Config
}

type ReshardArgs struct {
  Num int // config number
  //ShardNum int
  //Shard []string        // list of files in the shard
  Shards map[int][]string
  ShardHolders []string // list of servers holding this shard
  Seen map[int]map[int64]*Reply
  //Seen map[int64]*Reply
}

type ReshardReply struct {
  Err Err
  Shard []string // list of missing files for this shard
}

type RequestFilesArgs struct {
  Address string
  Num int // config number
  Files []string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

