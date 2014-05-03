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

func popularity(reads int, staleReads int, writes int) int {
  return reads + staleReads + writes
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
  Get = "Get"
  Put = "Put"
  Reconfig = "Reconfig"
  Reshard = "Reshard"
  Nop = "Nop"
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Id int64
}

//type PutReply struct {
  //Err Err
  //PreviousValue string   // For PutHash
//}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Id int64
}

//type GetReply struct {
type Reply struct {
  Err Err
  Value string
}

type ReconfigArgs struct {
  Config shardmaster.Config
}

type ReshardArgs struct {
  Num int // config number
  ShardNum int
  Shard map[string]string
  Seen map[int64]*Reply
}

type Popularity struct {
  Popularities map[int]int
  Config int
  Gid int64
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

