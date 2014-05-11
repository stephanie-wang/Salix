package shardmaster

import "crypto/rand"
import "math/big"
//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- replica group gid is joining, give it some shards.
// Leave(gid) -- replica group gid is retiring, hand off all its shards.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// Please don't change this file.
//

const (
  Join = "Join"
  Leave = "Leave"
  NShards = 10
  TChange = 2
)

type Config struct {
  Num int // config number
  Shards [NShards]int64 // shard -> gid
  Groups map[int64][]string // gid -> servers[]
}

type Popularity struct {
  Popularities map[int]int // shard # --> popularity score
  Config int
  Gid int64
  Seq int
  ID int64 
}

type JoinArgs struct {
  GID int64       // unique replica group ID
  Servers []string // group server ports
  ID int64
}

type JoinReply struct {
}

type LeaveArgs struct {
  GID int64
  ID int64
}

type LeaveReply struct {
}

type MoveArgs struct {
  Shard int
  GID int64
  ID int64
}

type MoveReply struct {
}

type QueryArgs struct {
  Num int // desired config number
  ID int64 // random ID number
}

type QueryReply struct {
  Config Config
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
