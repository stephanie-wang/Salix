/*
TODO: TestLots
--- FAIL: TestLots (25.13 seconds)
        test_test.go:35: decided values do not match; seq=12 i=3 v=1 v1=0
        test_test.go:35: decided values do not match; seq=12 i=3 v=1 v1=0
        test_test.go:35: decided values do not match; seq=4 i=2 v=9 v1=5
*/

package paxos

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"
import "log"

var PrintNdecided int = 0

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "px-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func ndecided(t *testing.T, pxa []*Paxos, seq int) int {
  count := 0
  var v interface{}
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      if decided {
        if count > 0 && v != v1 {
          t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
            seq, i, v, v1)
        }
        count++
        v = v1
      } else {
        if PrintNdecided > 0 {
          log.Printf("***[t] ndecided(seq=%v) not decided: %v\n", seq, i)
        }
      }
    }
  }
  
  if PrintNdecided > 0 {
    log.Printf("***[t] ndecided(seq=%v) = %v\n", seq, count)
  }
  
  return count
}

func waitn(t *testing.T, pxa[]*Paxos, seq int, wanted int) {
  to := 10 * time.Millisecond
  fmt.Println("waitn start:", seq)
  for iters := 0; iters < 30; iters++ {
    if ndecided(t, pxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, pxa, seq)
  if nd < wanted {
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
  fmt.Println("waitn end:", seq)
}

func waitmajority(t *testing.T, pxa[]*Paxos, seq int) {
  waitn(t, pxa, seq, (len(pxa) / 2) + 1)
}

func checkmax(t *testing.T, pxa[]*Paxos, seq int, max int) {
  time.Sleep(3 * time.Second)
  nd := ndecided(t, pxa, seq)
  if nd > max {
    t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
  }
}

func cleanup(pxa []*Paxos) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}

func startByClient(server int, pxa[]*Paxos, seq int, v interface{}) {
  go func() {
    leader := server
    for true {
      //fmt.Println("starting", server, seq, v);
      
      for pxa[leader] == nil {
        //select a new leader
        leader = (leader + 1) % len(pxa)
      }
      
      leader = pxa[leader].Start(seq, v)
      ok, _ := pxa[server].Status(seq)
      if ok {
        fmt.Printf("*** [t] done startByClient(seq=%d)\n", seq)
        break
      }
      time.Sleep(1 * time.Millisecond)
    }
  }()
}

func startByClientWithDeafServers(server int, pxa[]*Paxos, seq int, v interface{}, deaf []bool) {
  go func() {
    leader := server
    for true {
      //fmt.Println("starting", server, seq, v);
      
      for deaf[leader] || pxa[leader] == nil {
        //select a new leader
        leader = (leader + 1) % len(pxa)
      }
      
      leader = pxa[leader].Start(seq, v)
      ok, _ := pxa[server].Status(seq)
      if ok {
        fmt.Printf("*** [t] done startByClient(seq=%d)\n", seq)
        break
      }
      time.Sleep(1 * time.Millisecond)
    }
  }()
}

func noTestSpeed(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("time", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  t0 := time.Now()

  for i := 0; i < 20; i++ {
    startByClient(0, pxa, i, "x")
    waitn(t, pxa, i, npaxos)
  }

  d := time.Since(t0)
  fmt.Printf("20 agreements %v seconds\n", d.Seconds())
}

func TestBasic(t *testing.T) {
  runtime.GOMAXPROCS(4)

Debug = 1

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Single proposer ...\n")

  startByClient(0, pxa, 0, "hello")

  waitn(t, pxa, 0, npaxos)

  fmt.Printf("  ... Passed 1\n")

  fmt.Printf("Test: Many proposers, same value ...\n")

  for i := 0; i < npaxos; i++ {
    startByClient(i, pxa, 1, 77)
  }
  waitn(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed 2\n")

  fmt.Printf("Test: Many proposers, different values ...\n")

  startByClient(0, pxa, 2, 100)
  startByClient(1, pxa, 2, 101)
  startByClient(2, pxa, 2, 102)
  
  waitn(t, pxa, 2, npaxos)

  fmt.Printf("  ... Passed 3\n")

  fmt.Printf("Test: Out-of-order instances ...\n")

  startByClient(0, pxa, 7, 700)
  startByClient(0, pxa, 6, 600)
  startByClient(1, pxa, 5, 500)
  waitn(t, pxa, 7, npaxos)
  startByClient(0, pxa, 4, 400)
  startByClient(1, pxa, 3, 300)
  waitn(t, pxa, 6, npaxos)
  waitn(t, pxa, 5, npaxos)
  waitn(t, pxa, 4, npaxos)
  waitn(t, pxa, 3, npaxos)

//  if pxa[0].Max() != 7 {
//    t.Fatalf("wrong Max(): %d", pxa[0].Max())
//  }

  fmt.Printf("  ... Passed 4\n")
}

//TestDeaf: works with multi-paxos.
//If leader becomes deaf, it should elect leader to serve the start()
func TestDeaf(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("deaf", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Deaf proposer ...\n")

  startByClient(0, pxa, 0, "hello")
  waitn(t, pxa, 0, npaxos)

  deaf := make([]bool, npaxos)
  deaf[0] = true
  deaf[npaxos-1] = true
  os.Remove(pxh[0])
  os.Remove(pxh[npaxos-1])

  startByClientWithDeafServers(1, pxa, 1, "goodbye", deaf)
  waitmajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 2 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  deaf[0] = false;
  startByClientWithDeafServers(0, pxa, 1, "xxx", deaf)
  waitn(t, pxa, 1, npaxos-1)

  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos - 1 {
    t.Fatalf("a deaf peer heard about a decision")
  }

  startByClient(npaxos-1, pxa, 1, "yyy")
  waitn(t, pxa, 1, npaxos)

  fmt.Printf("  ... Passed 5\n")
}

func TestForget(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  const npaxos = 6
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gc", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m > 0 {
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  startByClient(0, pxa, 0, "00")
  startByClient(1, pxa, 1, "11")
  startByClient(2, pxa, 2, "22")
  startByClient(0, pxa, 6, "66")
  startByClient(1, pxa, 7, "77")

  waitn(t, pxa, 0, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  waitn(t, pxa, 1, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < npaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 1; i < npaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < npaxos; i++ {
    startByClient(i, pxa, 8 + i, "xx")
    waitn(t, pxa, 8 + i, npaxos)
  }
  
  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < npaxos; i++ {
      s := pxa[i].Min()
      fmt.Println(s)
      if s != 1 {
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(1 * time.Second)
  }
  if allok != true {
    t.Fatalf("Min() did not advance after Done()")
  }

  fmt.Printf("  ... Passed 6\n")
}

//
// does paxos forgetting actually free the memory?
//
func TestForgetMem(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Paxos frees forgotten instance memory ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)
  
  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gcmem", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  startByClient(0, pxa, 0, "x")
  waitn(t, pxa, 0, npaxos)

  runtime.GC()
  var m0 runtime.MemStats
  runtime.ReadMemStats(&m0)
  // m0.Alloc about a megabyte

  for i := 1; i <= 10; i++ {
    big := make([]byte, 1000000)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    startByClient(0, pxa, i, string(big))
    waitn(t, pxa, i, npaxos)
  }

  runtime.GC()
  var m1 runtime.MemStats
  runtime.ReadMemStats(&m1)
  // m1.Alloc about 90 megabytes

  for i := 0; i < npaxos; i++ {
    pxa[i].Done(10)
  }
  for i := 0; i < npaxos; i++ {
    startByClient(i, pxa, 11 + i, "z")
  }
  time.Sleep(3 * time.Second)
  for i := 0; i < npaxos; i++ {
    if pxa[i].Min() != 11 {
      t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
    }
  }

  runtime.GC()
  var m2 runtime.MemStats
  runtime.ReadMemStats(&m2)
  // m2.Alloc about 10 megabytes

  if m2.Alloc > (m1.Alloc / 2) {
    t.Fatalf("memory use did not shrink enough initial=%d final=%d", m1.Alloc, m2.Alloc)
  }

  fmt.Printf("  ... Passed 7\n")
}

func TestRPCCount(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: RPC counts aren't too high ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("count", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  ninst1 := 5
  seq := 0
  for i := 0; i < ninst1; i++ {
    startByClient(0, pxa, seq, "x")
    waitn(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total1 := 0
  for j := 0; j < npaxos; j++ {
    total1 += pxa[j].rpcCount
  }

  // per agreement:
  // 3 prepares
  // 3 accepts
  // 3 decides
  expected1 := ninst1 * npaxos * npaxos
  if total1 > expected1 {
    t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
      ninst1, total1, expected1)
  }

  ninst2 := 5
  for i := 0; i < ninst2; i++ {
    for j := 0; j < npaxos; j++ {
      go pxa[j].Start(seq, j + (i * 10))
    }
    waitn(t, pxa, seq, npaxos)
    seq++
  }

  time.Sleep(2 * time.Second)

  total2 := 0
  for j := 0; j < npaxos; j++ {
    total2 += pxa[j].rpcCount
  }
  total2 -= total1

  // worst case per agreement:
  // Proposer 1: 3 prep, 3 acc, 3 decides.
  // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  expected2 := ninst2 * npaxos * 15
  if total2 > expected2 {
    t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
      ninst2, total2, expected2)
  }

  fmt.Printf("  ... Passed 8\n")
}

//
// many agreements (without failures)
//
func TestMany(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  //defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("many", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    startByClient(i, pxa, 0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 5 active instances, to limit the
    // number of file descriptors.
    for seq >= 5 && ndecided(t, pxa, seq - 5) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      startByClient(i, pxa, seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed 9\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
// 
func TestOld(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Minority proposal ignored ...\n")

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("old", i)
  }

  pxa[1] = Make(pxh, 1, nil)
  pxa[2] = Make(pxh, 2, nil)
  pxa[3] = Make(pxh, 3, nil)
  startByClient(1, pxa, 1, 111)
  waitmajority(t, pxa, 1)

  pxa[0] = Make(pxh, 0, nil)
  startByClient(0, pxa, 1, 222)

  waitn(t, pxa, 1, 4)

  if false {
    pxa[4] = Make(pxh, 4, nil)
    waitn(t, pxa, 1, npaxos)
  }

  fmt.Printf("  ... Passed 10\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances, unreliable RPC ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("manyun", i)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
    startByClient(i, pxa, 0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && ndecided(t, pxa, seq - 3) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      startByClient(i, pxa, seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  
  fmt.Printf("  ... Passed 11\n")
}

func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "px-" + tag + "-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += strconv.Itoa(src) + "-"
  s += strconv.Itoa(dst)
  return s
}

func cleanpp(tag string, n int) {
  for i := 0; i < n; i++ {
    for j := 0; j < n; j++ {
      ij := pp(tag, i, j)
      os.Remove(ij)
    }
  }
}

func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
  cleanpp(tag, npaxos)

  pa := [][]int{p1, p2, p3}
  for pi := 0; pi < len(pa); pi++ {
    p := pa[pi]
    for i := 0; i < len(p); i++ {
      for j := 0; j < len(p); j++ {
        ij := pp(tag, p[i], p[j])
        pj := port(tag, p[j])
        err := os.Link(pj, ij)
        if err != nil {
          // one reason this link can fail is if the
          // corresponding Paxos peer has prematurely quit and
          // deleted its socket file (e.g., called px.Kill()).
          t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
        }
      }
    }
  }
}

func TestPartition(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  tag := "partition"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  seq := 0

  
  
  
  Debug = 0
  PrintNdecided = 0
  
  
  
  //time.Sleep(10000*time.Millisecond)

  fmt.Printf("Test: No decision if partitioned ...\n")

  part(t, tag, npaxos, []int{0,2}, []int{1,3}, []int{4})
  startByClient(1, pxa, seq, 111)
  checkmax(t, pxa, seq, 0)

  fmt.Printf("  ... Passed 12\n")

  fmt.Printf("Test: Decision in majority partition ...\n")

  part(t, tag, npaxos, []int{0}, []int{1,2,3}, []int{4})
  time.Sleep(2 * time.Second)
  
  waitmajority(t, pxa, seq)

  fmt.Printf("  ... Passed 13\n")



  fmt.Printf("Test: All agree after full heal ...\n")

  startByClient(0, pxa, seq, 1000) // poke them
  startByClient(4, pxa, seq, 1004)
  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})

  waitn(t, pxa, seq, npaxos)

  fmt.Printf("  ... Passed 14\n")

  fmt.Printf("Test: One peer switches partitions ...\n")

  fmt.Println("-----------------------------------------")
  

  //DPrintf("kobe")

  for iters := 0; iters < 20; iters++ {
    seq++

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    startByClient(0, pxa, seq, seq * 10)
    startByClient(3, pxa, seq, (seq * 10) + 1)
    waitmajority(t, pxa, seq)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }


    fmt.Println("*******************************************")
    
    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})
    waitn(t, pxa, seq, npaxos)
  }

  fmt.Printf("  ... Passed 15\n")

  fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

  for iters := 0; iters < 20; iters++ {
    seq++

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = true
    }

    part(t, tag, npaxos, []int{0,1,2}, []int{3,4}, []int{})
    for i := 0; i < npaxos; i++ {
      startByClient(i, pxa, seq, (seq * 10) + i)
    }
    waitn(t, pxa, seq, 3)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }
    
    part(t, tag, npaxos, []int{0,1}, []int{2,3,4}, []int{})

    for i := 0; i < npaxos; i++ {
      pxa[i].unreliable = false
    }

    waitn(t, pxa, seq, 5)
  }

  fmt.Printf("  ... Passed 16\n")
}

func TestLots(t *testing.T) {
  //return//

  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many requests, changing partitions ...\n")

  tag := "lots"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
    pxa[i].unreliable = true
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  done := false

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func(){ ch1 <- true }()
    for done == false {
      var a [npaxos]int
      for i := 0; i < npaxos; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < npaxos; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, npaxos, pa[0], pa[1], pa[2])
      time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
    }
  }()

  seq := 0

  // periodically start a new instance
  ch2 := make(chan bool)
  go func () {
    defer func() { ch2 <- true } ()
    for done == false {
      // how many instances are in progress?
      nd := 0
      for i := 0; i < seq; i++ {
        if ndecided(t, pxa, i) == npaxos {
          nd++
        }
      }
      if seq - nd < 10 {
        for i := 0; i < npaxos; i++ {
          startByClient(i, pxa, seq, rand.Int() % 10)
        }
        seq++
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  // periodically check that decisions are consistent
  ch3 := make(chan bool)
  go func() {
    defer func() { ch3 <- true }()
    for done == false {
      for i := 0; i < seq; i++ {
        ndecided(t, pxa, i)
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  time.Sleep(20 * time.Second)
  done = true
  <- ch1
  <- ch2
  <- ch3

  // repair, then check that all instances decided.
  for i := 0; i < npaxos; i++ {
    pxa[i].unreliable = false
  }
  part(t, tag, npaxos, []int{0,1,2,3,4}, []int{}, []int{})
  time.Sleep(5 * time.Second)

  for i := 0; i < seq; i++ {
    waitmajority(t, pxa, i)
  }

  fmt.Printf("  ... Passed 17\n")
}
