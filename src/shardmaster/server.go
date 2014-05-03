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
import "math"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num

  myDone int // higest seq of the Paxos log for which we have applied all operations
}


type Op struct {
  // Your data here.
  Type string // JOIN, LEAVE, POPULARITY, QUERY, NOP
  Request int64 // random ID for this particular request
  GID int64 // for JOIN, LEAVE, POPULARITY
  Seq int // for POPULARITY
  Servers []string // for JOIN
  // Shard int // for MOVE
  Scores map[int]int //for POPULARITY
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

    sm.newConfig(op.(Op))
    sm.myDone ++
  }
  sm.px.Done(sm.myDone)
}

// creates a new config if the op type is not QUERY or NOP
// moves the correct config if type is MOVE
// otherwise rebalances the shards by moving the least number
// possible
func (sm *ShardMaster) newConfig(op Op) {
  t := op.Type

  if t == "QUERY" || t=="NOP" {
    return
  }

  latest := sm.configs[len(sm.configs)-1]
  
  config := Config{}
  config.Num = latest.Num + 1
  config.Shards = latest.Shards

  // map is a reference and must be copied explicitly
  config.Groups = map[int64][]string{}

  for key, value := range latest.Groups{
    config.Groups[key] = value
  }

  // a MOVE just moves
  // TODO: don't need move anymore
  // if t == "MOVE"{
  //   config.Shards[op.Shard] = op.GID
  //   sm.configs = append(sm.configs, config)
  //   return
  // }

  if t == "JOIN"{
    config.Groups[op.GID] = op.Servers
  }

  // we should invalidate old group by (temporarily)
  // assigning the shards it contained to invalid group 0
  if t == "LEAVE" {
    delete(config.Groups, op.GID)
    for i, val := range config.Shards{
      if val == op.GID {
        config.Shards[i] = 0
      }
    }
  }

  // creates a freqency map with the group ID as the key
  // and the number of shards it stores as the value
  // Groups that don't store any shards right now will have frequency of 0
  freqs := sm.freq(config.Shards, config.Groups)

  // find the floor of the average number of shards
  // every group must store at least this number of shards
  low := int(math.Floor(float64(NShards)/float64(len(config.Groups))))

  // the number of groups that should store (low+1) shards
  numHigh := NShards - (len(config.Groups) * low)

  // this map used as a set by setting value to true
  // it contains the groups we find that store (low+1) shards
  // the length of this should be equal to numHigh when we are done
  foundHigh := make(map[int64]bool)

  // reassign some shards based on the balance of the groups
  for i, grp := range config.Shards {

    // invalid group is always reassigned
    if grp == 0 {
      assign := sm.laziest(freqs)
      config.Shards[i] = assign
      freqs[assign] = freqs[assign] + 1
    }

    num := freqs[grp]
    // assign is the group that currently has the least # of shards
    assign := sm.laziest(freqs)
    
    // only numHigh groups can have (low+1) shards
    if num == low + 1 {
      if len(foundHigh) > numHigh {
        config.Shards[i] = assign
        freqs[assign] = freqs[assign] + 1
        freqs[grp] = freqs[grp] -1
      } else {
        foundHigh[grp] = true
      }
    }

    // anything greater than (low+1) must be rebalanced in all cases
    if num > low+1 {
      config.Shards[i] = assign
      freqs[assign] = freqs[assign] + 1
      freqs[grp] = freqs[grp] -1
    }
  } 

  sm.configs = append(sm.configs, config)
}

// returns the group that is storing the fewest number of shards
// by looking through the frequency map of groups to the number of shards
// skips over group 0 (the invalid group)
func (sm *ShardMaster) laziest(f map[int64]int) int64{
  minVal := NShards+1
  var minGrp int64 = -1

  for grp, num := range f {
    if grp == 0 {
      continue
    }
    if num < minVal {
      minGrp = grp
      minVal = num
    }
  }

  return minGrp
}

// creates a freqency map with the group ID as the key
// and the number of shards it stores as the value
func (sm *ShardMaster) freq(shards [NShards]int64, groups map[int64][]string) map[int64]int {
  freq := make(map[int64]int)
  
  for grp, _ := range groups {
    freq[grp] = 0
  }

  for _, grp := range shards{
    if grp == 0 {
      continue
    }

    // freq[grp] should already be in our map because of
    // the 1st for loop
    freq[grp] = freq[grp]+1
  }
  return freq
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

func (sm *ShardMaster) PopularityPing(args *Popularity, reply *Popularity) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Type: "POPULARITY", GID:args.Gid, Seq:args.Seq, Scores:args.Popularities}
 
  seq := sm.propose(op, func(a Op, b Op) bool {
    if a.Type == b.Type && a.GID == b.GID && a.Seq == b.Seq{
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
