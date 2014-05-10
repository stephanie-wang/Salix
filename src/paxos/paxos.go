package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "math"

type ProposalNum struct {
  Hi int
  Low int
}

type PaxosInstance struct {
  mu sync.Mutex

  //proposer
  decided bool
  decidedVal interface{}
  highest_N ProposalNum
  
  //acceptor
  n_p ProposalNum
  n_a ProposalNum
  v_a interface{}
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  instances map[int]*PaxosInstance  //Paxos instances
  max int // number returned by Max()
  numPeers int
  majority int  //floor(numPeers/2) + 1
  
  doneVals []int  //done vals for each peer
}

type PrepareArgs struct {
  Seq int
  N ProposalNum
}

type PrepareReply struct {
  Ok bool //true=ok, false=reject
  N ProposalNum
  V interface{}  
}

type AcceptArgs struct {
  Seq int
  N ProposalNum
  V interface{}  
}

type AcceptReply struct {
  Ok bool //true=ok, false=reject
  //N ProposalNum   //proposer doesn't need to check this value
}

type DecidedArgs struct {
  Seq int
  V interface{}
  
  Me int
  DoneVal int
}

type DecidedReply struct {
  DoneVal int
}

// returns true if a > b
func ProposalNumGT(a ProposalNum, b ProposalNum) bool {
  if a.Hi > b.Hi {
    return true
  } else if a.Hi < b.Hi {
    return false
  } else {
    return a.Low > b.Low
  }
}

// returns px.instances[seq], creating it if necessary
func (px *Paxos) GetInstance(seq int) *PaxosInstance {
  px.mu.Lock()
  defer px.mu.Unlock()

  _, ok := px.instances[seq]
  if !ok {
    px.instances[seq] = MakeInstance()
  }
  return px.instances[seq]
}

// args = value, reply = pointer
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

// merge doneVals (received via RPC) with local doneVals
func (px *Paxos) MergeDoneVals(doneVal int, from int) {
  px.mu.Lock()
  defer px.mu.Unlock()
    
  if doneVal > px.doneVals[from] {
    px.doneVals[from] = doneVal
  }
}

// use function call if srv == me, otherwise use RPC call()
// also calls freememory
func (px *Paxos) Call2(srv string, name string, args interface{}, reply interface{}) bool {
  var err error
  
  if srv == px.peers[px.me] {
    if name == "Paxos.Prepare" {
      prepareArgs := args.(PrepareArgs)
      prepareReply := reply.(*PrepareReply)
      err = px.Prepare(&prepareArgs, prepareReply)
    } else if name == "Paxos.Accept" {
      acceptArgs := args.(AcceptArgs)
      acceptReply := reply.(*AcceptReply)
      err = px.Accept(&acceptArgs, acceptReply)
    } else if name == "Paxos.Decided" {
      decidedArgs := args.(DecidedArgs)
      decidedReply := reply.(*DecidedReply)
      err = px.Decided(&decidedArgs, decidedReply)
    } else {
      return false
    }
    if err == nil {
      return true
    }

    fmt.Println(err)
    return false
  } else {
    result := call(srv, name, args, reply)
    return result
  }
}

func (px *Paxos) proposer(seq int, v interface{}) {
  inst := px.GetInstance(seq)  
  
  for !inst.decided && !px.dead {
    inst.mu.Lock()
    var n ProposalNum = ProposalNum{inst.highest_N.Hi + 1, px.me}
    inst.highest_N = n  //ensures next iter will use even higher proposal number
    inst.mu.Unlock()
    
    //send Prepare to all peers
    
    var numPrepareOks int = 0
    var numPrepareRejects int = 0
    var v_prime interface{} = v
    var highest_n ProposalNum = ProposalNum{-1, -1}
    
    for i:=0; i<px.numPeers && !px.dead; i++ {
        prepareArgs := PrepareArgs{}
        prepareArgs.Seq = seq
        prepareArgs.N = n
        prepareReply := PrepareReply{}
        
        if !px.Call2(px.peers[i], "Paxos.Prepare", prepareArgs, &prepareReply) {
          //treat RPC failure as prepareReject
          prepareReply.Ok = false
        }
        
        if prepareReply.Ok {
          numPrepareOks = numPrepareOks + 1

          //keep track of highest prepareOk proposal number / value
          if ProposalNumGT(prepareReply.N, highest_n) {
            highest_n = prepareReply.N
            v_prime = prepareReply.V
          }

          //track highest proposal num seen
          inst.mu.Lock()
          if ProposalNumGT(prepareReply.N, inst.highest_N) {
            inst.highest_N = prepareReply.N
          }
          inst.mu.Unlock()
        } else {
          numPrepareRejects = numPrepareRejects + 1
        }
        
        //abort early if enough oks or too many rejects/failures
        if numPrepareOks >= px.majority {
          break
        }
        if numPrepareRejects > px.numPeers - px.majority {
          break
        }
    }
    
    if numPrepareOks >= px.majority {
      //send Accept to all peers
      
      var numAcceptOks int = 0
      var numAcceptRejects int = 0
      
      for i:=0; i<px.numPeers && !px.dead; i++ {
        acceptArgs := AcceptArgs{}
        acceptArgs.Seq = seq
        acceptArgs.N = n
        acceptArgs.V = v_prime
        acceptReply := AcceptReply{}
        
        if !px.Call2(px.peers[i], "Paxos.Accept", acceptArgs, &acceptReply) {          
          //treat RPC failure as acceptReject
          acceptReply.Ok = false
        }
          
        if acceptReply.Ok {
          numAcceptOks = numAcceptOks + 1          
        } else {
          numAcceptRejects = numAcceptRejects + 1
        }
        
        //abort early if enough oks or too many rejects/failures
        if numAcceptOks >= px.majority {
          break
        }
        if numAcceptRejects > px.numPeers - px.majority {
          break
        }
      }
            
      if numAcceptOks >= px.majority {
        //send Decided to all peers
        
        for i:=0; i<px.numPeers && !px.dead; i++ {
          decidedArgs := DecidedArgs{}
          decidedArgs.Seq = seq
          decidedArgs.V = v_prime
          decidedArgs.Me = px.me
          decidedArgs.DoneVal = px.doneVals[px.me]
          decidedReply := DecidedReply{}
          if px.Call2(px.peers[i], "Paxos.Decided", decidedArgs, &decidedReply) {
            px.MergeDoneVals(decidedReply.DoneVal, i)
          }
        }
      }
    }
  }
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  if args.Seq < px.Min() {
    return nil
  }
  
  inst := px.GetInstance(args.Seq)
  
  inst.mu.Lock()
  if ProposalNumGT(args.N, inst.n_p) {
    inst.n_p = args.N
    reply.N = inst.n_a
    reply.V = inst.v_a
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  inst.mu.Unlock()
  
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {  
  if args.Seq < px.Min() {
    return nil
  }
  
  inst := px.GetInstance(args.Seq)
  
  inst.mu.Lock()
  if ProposalNumGT(args.N, inst.n_p) || args.N == inst.n_p {
    inst.n_p = args.N
    inst.n_a = args.N
    inst.v_a = args.V
    //reply.N = args.N  //not needed by proposer()
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  inst.mu.Unlock()
  
  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.MergeDoneVals(args.DoneVal, args.Me)
  reply.DoneVal = px.doneVals[px.me]

  if args.Seq < px.Min() {
    return nil
  }
  
  inst := px.GetInstance(args.Seq)
  
  //IMPORTANT: DO NOT CHANGE v_a!!! set decidedVal instead
  
  inst.mu.Lock()
  inst.decided = true
  inst.decidedVal = args.V
  inst.mu.Unlock()
  
  return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  if seq > px.max {
    px.max = seq
  }
  px.mu.Unlock()
  
  go px.proposer(seq, v)
}

func (px *Paxos) FreeMemory(keepAtLeast int) {
  for i:=0; i<keepAtLeast; i++ {
    px.mu.Lock()
    delete(px.instances, i)
    px.mu.Unlock()
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  if seq > px.doneVals[px.me] {
    px.doneVals[px.me] = seq
  }
  px.mu.Unlock()
  
  px.Min()  //this will force a free memory
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  px.mu.Lock()
  minDoneVal := math.MaxInt32
  for i:=0; i<px.numPeers; i++ {
    if px.doneVals[i] < minDoneVal {
      minDoneVal = px.doneVals[i]
    }
  }
  px.mu.Unlock()
  
  retVal := 1 + minDoneVal
  px.FreeMemory(retVal)
  
  return retVal
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  inst, ok := px.instances[seq]
  px.mu.Unlock()
  
  var retDecided bool = false
  var retVal interface{} = nil
  
  if ok {
    inst.mu.Lock()
    if inst.decided {
      retDecided = true
      retVal = inst.decidedVal
    }
    inst.mu.Unlock()
  }
  
  return retDecided, retVal
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

func MakeInstance() *PaxosInstance {
  inst := &PaxosInstance{}

  inst.decided = false
  inst.highest_N = ProposalNum{-1, -1}
  
  inst.n_p = ProposalNum{-1, -1}
  inst.n_a = ProposalNum{-1, -1}
  inst.v_a = nil
  
  return inst
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  px.instances = make(map[int]*PaxosInstance)
  px.max = -1
  px.numPeers = len(peers)
  px.majority = px.numPeers / 2 + 1
  px.doneVals = make([]int, px.numPeers)
  for i:=0; i<px.numPeers; i++ {
    px.doneVals[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}