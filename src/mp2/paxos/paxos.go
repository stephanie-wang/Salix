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
import "time"

var Debug int = 0

func Dprintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

var PrintFullVal bool = true

func valStr(v interface{}) interface{} {
  if PrintFullVal {
    return v
  } else {
    return "(v)"
  }
}

var FAILURE_DETECTOR_TO = 100  //ms

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  
  numPeers int
  majority int  //floor(numPeers/2) + 1

  view int
  instances map[int]*PaxosInstance  //Paxos instances
  max int // number returned by Max()  
  doneVals []int  //done vals for each peer

  //duplicate detection for Start()
  started map[int]bool
  startMu sync.Mutex

  //failure detection
  fdMu sync.Mutex
  fdLastPing time.Time
}

func InitPx(px *Paxos) {
  px.numPeers = len(px.peers)
  px.majority = px.numPeers / 2 + 1

  px.view = 0
  px.instances = make(map[int]*PaxosInstance)
  px.max = -1
  px.doneVals = make([]int, px.numPeers)
  for i:=0; i<px.numPeers; i++ {
    px.doneVals[i] = -1
  }
  
  px.started = make(map[int]bool)
  
  px.fdLastPing = time.Now()
}

type PaxosInstance struct {
  mu sync.Mutex
  
  //acceptor
  Accepted bool
  View_a int
  V_a interface{}
  
  //listener
  Decided bool
  DecidedVal interface{}  
}

func MakeInstance() *PaxosInstance {
  inst := &PaxosInstance{}

  inst.Accepted = false
  inst.View_a = 0
  inst.V_a = nil
  
  inst.Decided = false
  inst.DecidedVal = nil
  
  return inst
}

type PrepareArgs struct {  
  View int
  LowestUndecided int
  Me int
}

type PrepareReply struct {
  Ok bool //true=ok, false=reject
  Accepted map[int]PaxosInstance  //includes decided so
            //later leaders can re-propose with same accepted value
  View int
}

type AcceptArgs struct {
  Seq int
  View int
  V interface{}  
  Me int
}

type AcceptReply struct {
  Ok bool //true=ok, false=reject
  View int
}

type ProbeArgs struct {
  Seq int
  Me int
}

type ProbeReply struct {
  Decided bool
  DecidedVal interface{}
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

// returns px.instances[seq], creating it if necessary
func (px *Paxos) GetInstance(seq int) *PaxosInstance {
  px.mu.Lock()
  defer px.mu.Unlock()

  return px.GetInstanceNoLock(seq)
}

func (px *Paxos) GetInstanceNoLock(seq int) *PaxosInstance {
  _, ok := px.instances[seq]
  if !ok {
    px.instances[seq] = MakeInstance()
  }
  return px.instances[seq]
}

// merge doneVals (received via RPC) with local doneVals
func (px *Paxos) MergeDoneVals(doneVal int, from int) {
  px.mu.Lock()
  defer px.mu.Unlock()
    
  if doneVal > px.doneVals[from] {
    px.doneVals[from] = doneVal
  }
}

func (px *Paxos) leader(view int) int {
  return view % len(px.peers)
}

func (px *Paxos) lowestUndecided() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  slot := 0   //px.Min()
  for {
    inst := px.GetInstanceNoLock(slot)
    inst.mu.Lock()
    defer inst.mu.Unlock()
    if !inst.Decided {
      return slot
    }
    slot += 1
  }

  return math.MaxInt32  //should never get here
}

func (px *Paxos) preparer(view int) {
}

func (px *Paxos) driver(seq int, v interface{}) {
}

func (px *Paxos) proposer(seq int, v interface{}) {
}

func (px *Paxos) prober(seq int) {
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.fdHearFrom(args.Me)
  
  if args.View >= px.view {
    if args.Me != px.me {
      px.view = args.View
    }
    
    //reply.Accepted is used by the peer when determining
    //the highest View_a of an instance
    reply.Accepted = make(map[int]PaxosInstance)
    
    if args.Me != px.me {
      px.mu.Lock()
      defer px.mu.Unlock()
    }
    
    for slot,inst := range px.instances {
      inst.mu.Lock()
      if slot >= args.LowestUndecided && inst.Accepted {
        reply.Accepted[slot] = *inst
      }
      inst.mu.Unlock()
    }
    reply.Ok = true
  } else {
    reply.Ok = false;
  }
  reply.View = px.view
  
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {  
  if args.Seq < px.Min() {
    return nil
  }
  
  px.fdHearFrom(args.Me)
  
  inst := px.GetInstanceNoLock(args.Seq)
  
  if args.Me != px.me {
    px.mu.Lock()
    defer px.mu.Unlock()
  }
  
  inst.mu.Lock()

  if args.View == px.view {
    px.view = args.View
    inst.View_a = args.View
    inst.V_a = args.V
    inst.Accepted = true
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  reply.View = px.view

  inst.mu.Unlock()
  
  return nil;
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  return nil
}

func (px *Paxos) Probe(args *ProbeArgs, reply *ProbeReply) error {
  return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

func (px *Paxos) Start(seq int, v interface{}) int {
  px.startMu.Lock()
  defer px.startMu.Unlock()
  
  px.mu.Lock()
  if seq > px.max {
    px.max = seq
  }
  px.mu.Unlock()
  
  _, ok := px.started[seq]
  if !ok {
    px.started[seq] = true
    go px.driver(seq, v)
  }
  
  return px.leader(px.view)
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
    if inst.Decided {
      retDecided = true
      retVal = inst.DecidedVal
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

//
// failure detection code
//

func (px *Paxos) fdThread() {
  fdPrevTarget := -1
  fdInstalledView := -1
  var prevLastPing time.Time
  
  for !px.dead {
    time.Sleep(time.Duration(FAILURE_DETECTOR_TO) * time.Millisecond)
    
    failure := false
    view := px.view
    
    px.fdMu.Lock()
    target := px.leader(view)
    if target == fdPrevTarget {
      failure = (target != px.me) && (prevLastPing == px.fdLastPing)
    }
    fdPrevTarget = target
    prevLastPing = px.fdLastPing
    px.fdMu.Unlock()
    
    if failure {
      if view > fdInstalledView {
        px.fdOnFail(view)
        fdInstalledView = view
      }
    }
  }
}

func (px *Paxos) fdHearFrom(host int) {
  px.fdMu.Lock()
  defer px.fdMu.Unlock()
  
  if host == px.leader(px.view) {
    px.fdLastPing = time.Now()
  }
}

func (px *Paxos) fdOnFail(view int) {
  if px.view == view {
    for px.leader(view) != px.me {
      view += 1
    }
    go px.preparer(view)
  }
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

  InitPx(px)

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

  //start the failure detector
  go px.fdThread();

  return px
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
