package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num

  myDone int // higest seq of the Paxos log for which we have applied all operations

  scores [NShards]int // shard # --> popularity score; score can correspond to any config
  latestHeard map[int64]int // group id --> highest seq # for this current configuration
  // if the group has not sent a score for the current config, it will not be here
}


type Op struct {
  Type string // JOIN, LEAVE, POPULARITY, QUERY, NOP
  Request int64 // random ID for this particular request
  GID int64 // for JOIN, LEAVE, POPULARITY
  Seq int // for POPULARITY
  ConfigNum int // for POPULARITY
  Servers []string // for JOIN
  Scores map[int]int //for POPULARITY
  Shard int // for MOVE
  Num int // for QUERY
}

// updates sm.configs by applying outstanding Ops until
// it is at a point where the queried config # can be determined
// if queried is -1, then all updates until (and including) seq
// will be applied
func (sm *ShardMaster) update(seq int, queried int) {

  for i:=sm.myDone+1; i<=seq; i++ {
    if queried != -1 && len(sm.configs) > queried {
      return
    }

    decided, op := sm.px.Status(i)
    if !decided {
      sm.px.Start(i, Op{Type: "NOP"})
      op = sm.wait(i, true)
      if sm.dead {
        return
      }
    }

    sm.execute(op.(Op))
    sm.myDone ++
  }

  sm.px.Done(sm.myDone)
}

// executes a particular op in the log
func (sm *ShardMaster) execute(op Op) {
  if op.Type == "NOP" || op.Type == "QUERY" {
    return
  }

  current := sm.configs[len(sm.configs)-1]

  // popularity score is only valid if it's the current configuration
  if op.Type == "POPULARITY" && current.Num == op.ConfigNum {
    seq, ok := sm.latestHeard[op.GID]
    
    // only save score if we haven't heard from group before
    // or if the sequence number from this group is higher than before
    if !ok || op.Seq > seq {
      sm.latestHeard[op.GID] = op.ConfigNum
      for shard, score := range op.Scores {
        sm.scores[shard] = score
      }
    }

    // all groups have reported values for this config
    if len(sm.latestHeard) == len(current.Groups) {
      
      newGroups := make(map[int64][]string)
      for gid, servers := range current.Groups {
        newGroups[gid] = servers
      }
      sm.createNewConfig(newGroups)

      // none of the score reports are valid w/new config
      sm.latestHeard = make(map[int64]int)
    }

  return
  }

  newGroups := make(map[int64][]string)
  for gid, servers := range current.Groups {
    newGroups[gid] = servers
  }

  if op.Type == "JOIN" {
    newGroups[op.GID] = op.Servers
    sm.createNewConfig(newGroups)
    
    // none of the score reports are valid w/new config
    sm.latestHeard = make(map[int64]int)
  }

  if op.Type == "LEAVE" {
    delete(newGroups, op.GID)
    sm.createNewConfig(newGroups)

    // none of the score reports are valid w/new config
    sm.latestHeard = make(map[int64]int)
  }
}

// creates a new configuration with the popularity scores in sm.scores
// optimizes to make every group have partitions w/an equal sum of popularities
// appends the new config at the end of sm.configs
func (sm *ShardMaster) createNewConfig(groups map[int64][]string) {
  last := sm.configs[len(sm.configs)-1]
  
  newGroups := make(map[int64]bool)
  for grp, _ := range groups {
    newGroups[grp] = true
  }
  
  newShards := loadBalance(sm.scores, last.Shards, newGroups)
  newConfig := Config{Num: last.Num+1, Shards: newShards, Groups: groups}
  sm.configs = append(sm.configs, newConfig)
}


// RPC Join from client
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // many clients can be calling this
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Type:"JOIN", GID:args.GID, Servers:args.Servers}
  seq := sm.propose(op, func(a Op, b Op) bool {
    if a.Type == b.Type && a.GID == b.GID {
      return true
    }
    return false
    })

  sm.update(seq, -1)

  return nil
}

// RPC Leave from client
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // many clients can be calling this
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Type:"LEAVE", GID: args.GID}
  sm.propose(op, func(a Op, b Op) bool {
    if a.Type == b.Type && a.GID == b.GID {
      return true
    }
    return false
    })

  return nil
}

// RPC Move from client
 func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
   // many clients can be calling this
   sm.mu.Lock()
   defer sm.mu.Unlock()

   op := Op{Type:"MOVE", GID: args.GID, Shard: args.Shard}
   seq := sm.myDone + 1

   for {
     sm.px.Start(seq, op)
     temp := sm.wait(seq, false)
     if sm.dead {
       return nil
     }
     decided := temp.(Op)
     // decided := sm.wait(seq, false).(Op)
     if decided.Type == "MOVE" && decided.GID == args.GID && decided.Shard == args.Shard {
       break
     }
     seq ++ 
   }
   return nil
 }

// RPC Query from client
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // many clients can be calling this
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Type:"QUERY", Num: args.Num}

  seq := sm.propose(op, func(a Op, b Op) bool {
    if a.Type == b.Type && a.Num == b.Num {
      return true
    }
    return false
    })

  sm.update(seq, args.Num)
  
  if args.Num == -1 {
    reply.Config = sm.configs[len(sm.configs)-1]
  } else {
    if args.Num >= len(sm.configs) {
      reply.Config = Config{}
    } else {
      reply.Config = sm.configs[args.Num]    
    }
  }
  

  // create an op from args + call the handler
  // update until we can return the right configuration
  // reply with the right configuration
  return nil
}

// RPC Popularity ping from client
func (sm *ShardMaster) PopularityPing(args *Popularity, reply *Popularity) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Type: "POPULARITY", GID:args.Gid, Seq:args.Seq, Scores:args.Popularities, ConfigNum: args.Config}
 
  seq := sm.propose(op, func(a Op, b Op) bool {
    if a.Type == b.Type && a.GID == b.GID && a.Seq == b.Seq && a.ConfigNum == b.ConfigNum {
      return true
    }
    return false
    })

  sm.update(seq, -1)
  return nil
}

// Puts op into a slot in the Paxos log and returns the slot number.
// Uses equals to figure out whether the op has been put successfully in the log.
func (sm *ShardMaster) propose(op Op, equals func(a Op, b Op) bool) int {
  seq := sm.myDone + 1

  for {
    sm.px.Start(seq, op)
    temp := sm.wait(seq, false)
    if sm.dead {
      return -1
    }
    decided := temp.(Op)
    if equals(decided, op) {
      break
    }
    seq ++
  }

  return seq
}

// waits an increasing timeperiod for a particular Paxos instance 
// to decide on the operation for a given seq
// when it has been decided, it returns the operation
func (sm *ShardMaster) wait(seq int, isNOP bool) interface{} {
  to := 10 * time.Millisecond
  
  for {
    
    // longstanding thread must die!!
    if sm.dead {
      break
    }
    
    // we should never be waiting for a seq to finish if we've executed past that
    // if this ever happens, our concurrency is messed up
    // returning nil causes a null pointer reference and makes go panic
    // then we can debug
    if seq < sm.myDone && !isNOP{
      return nil
    }

    decided, operation := sm.px.Status(seq)
    if decided {
      return operation
    }

    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  for i:=0; i<NShards; i++{
    sm.configs[0].Shards[i] = 0
  }
  sm.configs[0].Num = 0

  sm.myDone = -1

  for i := 0; i<NShards; i++ {
    sm.scores[i] = 0
  }

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
