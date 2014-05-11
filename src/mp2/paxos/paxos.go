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

  mu2 sync.RWMutex  //used to make sure no proposals during election?
  
  dataMu sync.Mutex
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
  DoneVal int
}

type ProbeReply struct {
  Decided bool
  DecidedVal interface{}
  DoneVal int
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
  px.dataMu.Lock()
  defer px.dataMu.Unlock()

  _, ok := px.instances[seq]
  if !ok {
    px.instances[seq] = MakeInstance()
  }
  return px.instances[seq]
}

// merge doneVals (received via RPC) with local doneVals
func (px *Paxos) MergeDoneVals(doneVal int, from int) {
  px.dataMu.Lock()
  defer px.dataMu.Unlock()
    
  if doneVal > px.doneVals[from] {
    px.doneVals[from] = doneVal
  }
}

func (px *Paxos) leader(view int) int {
  return view % px.numPeers
}

func (px *Paxos) lowestUndecided() int {
  slot := 0
  for {
    inst := px.GetInstance(slot)
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
  Dprintf("***[%v][view=%v] preparer(view=%v)\n", px.me, px.view, view)
  
  for !px.dead {
    px.mu.Lock()
    x := px.preparer_iteration(view)
    px.mu.Unlock()
    if !x {
      break
    }
  }
}

//returns true for continue
func (px *Paxos) preparer_iteration(view int) bool {
  //this breaks code
  //px.mu2.Lock()
  //defer px.mu2.Unlock()

  //px.mu.Lock()
  //defer px.mu.Unlock()

  if px.view >= view {
    return false
  }

  //send Prepare to all peers
  var numPrepareOks int = 0
  var numPrepareRejects int = 0
  lowestUndecided := px.lowestUndecided()
  replies := make([]PrepareReply, 0)
  
  for i:=0; i<px.numPeers && !px.dead; i++ {
    prepareArgs := PrepareArgs{}
    prepareArgs.View = view;
    prepareArgs.LowestUndecided = lowestUndecided
    prepareArgs.Me = px.me
    prepareReply := PrepareReply{}
    
    if !px.Call2(px.peers[i], "Paxos.Prepare", prepareArgs, &prepareReply) {
      //treat RPC failure as prepareReject
      numPrepareRejects = numPrepareRejects + 1
    } else {
      px.fdHearFrom(i)
      if prepareReply.Ok {
        numPrepareOks = numPrepareOks + 1
        replies = append(replies, prepareReply)
      } else if prepareReply.View > px.view {     //TODO: might not be needed
        //someone else became leader
        return false
      }
    }
    
    //abort early if enough oks or too many rejects/failures
    if numPrepareOks >= px.majority || numPrepareRejects > px.numPeers - px.majority {
      break
    }
  }
  
  if numPrepareOks >= px.majority {
    //px.mu.Lock()
    
    px.view = view
    
    //changed := make([]int, 0) //for debugging
    
    for _, reply := range replies {
      if reply.Accepted != nil {
        for slot, recvd := range reply.Accepted {
          inst := px.GetInstance(slot)
          inst.mu.Lock()
          
          if !inst.Decided {
            if !inst.Accepted ||
                 //recvd.Decided ||
                 (recvd.Accepted && recvd.View_a >= inst.View_a) {  //put equal here to get my own

              //don't overwrite my decided value!
              inst.Accepted = recvd.Accepted
              inst.View_a = recvd.View_a
              inst.V_a = recvd.V_a
              
              //changed = append(changed, slot)
            }
          }
          
          inst.mu.Unlock()
        }
      }
    }
    
/*
    for _, slot := range changed {
      inst := px.GetInstance(slot)
      inst.mu.Lock()
      //inst.View_a = view  //new view
      
      //log.Printf("***[%v][view=%v] highest accept seq=%v, val=%v, view_a=%v preparer(view=%v)\n", px.me, inst.View_a, slot, inst.V_a, px.view, view)
      
      inst.mu.Unlock()
    }
*/
    
    //start each accepted instance that I didn't know about
    px.dataMu.Lock()
    for slot, inst := range px.instances {
      inst.mu.Lock()        
      if (inst.Accepted && !inst.Decided) {
        go px.Start(slot, inst.V_a) //TODO: might be a bug
        //do that in thread so I don't have to give up px.mu
      }
      inst.mu.Unlock()
    }
    px.dataMu.Unlock()
    
    //px.mu.Unlock()
    
    return false
  }
  
  return true
}

func (px *Paxos) driver(seq int, v interface{}) {
  Dprintf("***[%v][view=%v] driver(seq=%v, v=%v)\n", px.me, px.view, seq, valStr(v))
  
  inst := px.GetInstance(seq)
  
  for !inst.Decided && !px.dead {   
    //this breaks code
    //px.mu2.RLock()
    //defer px.mu2.RUnlock()
    
    view := px.view
    leader := px.leader(view)
    if leader == px.me {
      px.propose(view, seq, v)
    } else {
      px.probe(view, seq)
    }
  }
}

func (px *Paxos) propose(view int, seq int, v interface{}) {
  
  inst := px.GetInstance(seq)
  
  v_prime := v;
  
  px.mu.Lock()
  inst.mu.Lock()
  if inst.Accepted {
    v_prime = inst.V_a
  }
  inst.mu.Unlock()
  px.mu.Unlock()
  
  //Dprintf("***[%v][view=%v] proposer(view=%v, seq=%v, v_prime=%v)\n", px.me, px.view, view, seq, valStr(v_prime))
  
  //send Accept to all peers
  numAcceptOks := 0
  numAcceptRejects := 0
  
  for i:=0; i<px.numPeers && !px.dead; i++ {
    acceptArgs := AcceptArgs{}
    acceptArgs.Seq = seq
    acceptArgs.View = view //view //x //view
    acceptArgs.V = v_prime
    acceptArgs.Me = px.me
    acceptReply := AcceptReply{}
    
    if !px.Call2(px.peers[i], "Paxos.Accept", acceptArgs, &acceptReply) {          
      acceptReply.Ok = false  //treat RPC failure as acceptReject
    } else {
      px.fdHearFrom(i)
    }
    
    if px.view > view { //if proposal view is obsolete
      return
    }

    if acceptReply.View > view {  //if other nodes have moved on
      px.view = acceptReply.View  //also move on
      return
    }
    
    if acceptReply.Ok {
      numAcceptOks = numAcceptOks + 1          
    } else {
      numAcceptRejects = numAcceptRejects + 1
    }
    
    //abort early if enough oks or too many rejects/failures
    if numAcceptOks >= px.majority || numAcceptRejects > px.numPeers - px.majority{
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
        px.fdHearFrom(i)
        px.MergeDoneVals(decidedReply.DoneVal, i)
      }
    }
  }
}

func (px *Paxos) probe(view int, seq int) {
  //Dprintf("***[%v][view=%v] prober(view=%v, seq=%v)\n", px.me, px.view, view, seq)
  
  for i:=0; i<px.numPeers && !px.dead; i++ {
    probeArgs := ProbeArgs{}
    probeArgs.Seq = seq
    probeArgs.Me = px.me
    probeArgs.DoneVal = px.doneVals[px.me]
    probeReply := ProbeReply{}
    
    if px.Call2(px.peers[i], "Paxos.Probe", probeArgs, &probeReply) {
      px.MergeDoneVals(probeReply.DoneVal, i)
      
      if probeReply.Decided {
        for i:=0; i<px.numPeers && !px.dead; i++ {
          decidedArgs := DecidedArgs{}
          decidedArgs.Seq = seq
          decidedArgs.V = probeReply.DecidedVal
          decidedArgs.Me = px.me
          decidedArgs.DoneVal = px.doneVals[px.me]
          decidedReply := DecidedReply{}
          
          if px.Call2(px.peers[i], "Paxos.Decided", decidedArgs, &decidedReply) {
            px.fdHearFrom(i)
          }
        }
        
        return
      }
    }      
  }
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  Dprintf("***[%v][view=%v] onPrepare(args.View=%v, args.Lowest=%v) from %v\n", px.me, px.view, args.View, args.LowestUndecided, args.Me)
  
  px.fdHearFrom(args.Me)
  
  if args.View >= px.view {
    if args.Me != px.me {
      px.view = args.View
    }
    
    //reply.Accepted is used by the peer when determining
    //the highest View_a of an instance
    reply.Accepted = make(map[int]PaxosInstance)
    
    px.dataMu.Lock()
    for slot,inst := range px.instances {
      inst.mu.Lock()
      if slot >= args.LowestUndecided && inst.Accepted {
        reply.Accepted[slot] = *inst
      }
      inst.mu.Unlock()
    }
    px.dataMu.Unlock()
    
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
  
  inst := px.GetInstance(args.Seq)
  
  inst.mu.Lock()

  if args.View >= px.view {
    Dprintf("***[%v][view=%v] accepted onAccept(args.View=%v, args.Seq=%v, args.V=%v) from %v\n", px.me, px.view, args.View, args.Seq, valStr(args.V), args.Me)
    
    px.view = args.View
    inst.View_a = args.View
    inst.V_a = args.V
    inst.Accepted = true
    reply.Ok = true
  } else {
    Dprintf("***[%v][view=%v] rejected onAccept(args.View=%v, args.Seq=%v, args.V=%v) from %v\n", px.me, px.view, args.View, args.Seq, valStr(args.V), args.Me)
    reply.Ok = false
  }
  reply.View = px.view

  inst.mu.Unlock()
  
  return nil;
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.MergeDoneVals(args.DoneVal, args.Me)
  reply.DoneVal = px.doneVals[px.me]
  
  if args.Seq < px.Min() {
    return nil
  }
  
  Dprintf("***[%v][view=%v] onDecided(args.Seq=%v, args.V=%v) from %v\n", px.me, px.view, args.Seq, valStr(args.V), args.Me)
  
  px.fdHearFrom(args.Me)
  
  inst := px.GetInstance(args.Seq)
  
  //IMPORTANT: DO NOT CHANGE v_a!!! set DecidedVal instead  
  inst.mu.Lock()
  inst.Decided = true
  inst.DecidedVal = args.V
  inst.mu.Unlock()
  
  return nil
}

func (px *Paxos) Probe(args *ProbeArgs, reply *ProbeReply) error {
  px.MergeDoneVals(args.DoneVal, args.Me)
  reply.DoneVal = px.doneVals[px.me]

  if args.Seq < px.Min() {
    return nil
  }
  
  inst := px.GetInstance(args.Seq)

  inst.mu.Lock()
  if inst.Decided {
    reply.Decided = true
    reply.DecidedVal = inst.DecidedVal
    Dprintf("***[%v][view=%v] found! onProbe(args.Seq=%v) from %v\n", px.me, px.view, args.Seq, args.Me)
  }
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

func (px *Paxos) Start(seq int, v interface{}) int {
  px.startMu.Lock()
  defer px.startMu.Unlock()
  
  if seq > px.max {
    px.max = seq
  }
  
  _, ok := px.started[seq]
  if !ok {
    px.started[seq] = true
    go px.driver(seq, v)
  }
  
  return px.leader(px.view)
}

func (px *Paxos) FreeMemory(keepAtLeast int) {
  for i:=0; i<keepAtLeast; i++ {
    px.dataMu.Lock()
    delete(px.instances, i)
    px.dataMu.Unlock()
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.dataMu.Lock()
  if seq > px.doneVals[px.me] {
    px.doneVals[px.me] = seq
  }
  px.dataMu.Unlock()
  
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
  px.dataMu.Lock()
  minDoneVal := math.MaxInt32
  for i:=0; i<px.numPeers; i++ {
    if px.doneVals[i] < minDoneVal {
      minDoneVal = px.doneVals[i]
    }
  }
  px.dataMu.Unlock()
  
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
  px.dataMu.Lock()
  inst, ok := px.instances[seq]
  px.dataMu.Unlock()
  
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
      //avoid calling fdOnFail multiple times for same view #
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
  }
  if px.view >= view { //if new view has been reached already
    return             //avoid starting new thread
  }
  go px.preparer(view)
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
