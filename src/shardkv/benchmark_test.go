package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
// import "path"
import "time"
import "fmt"

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "skv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func mcleanup(sma []*shardmaster.ShardMaster) {
  for i := 0; i < len(sma); i++ {
    if sma[i] != nil {
      sma[i].Kill()
    }
  }
}

func cleanup(sa [][]*ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].Kill()
    }
  }
}

func setup(tag string, unreliable bool, loadBalance bool) ([]string, []int64, [][]string, [][]*ShardKV, func()) {
  runtime.GOMAXPROCS(4)
  
  const nmasters = 3
  var sma []*shardmaster.ShardMaster = make([]*shardmaster.ShardMaster, nmasters)
  var smh []string = make([]string, nmasters)
  // defer mcleanup(sma)
  for i := 0; i < nmasters; i++ {
    smh[i] = port(tag+"m", i)
  }
  for i := 0; i < nmasters; i++ {
    sma[i] = shardmaster.StartServer(smh, i)
  }

  const ngroups = 2   // replica groups
  const nreplicas = 3 // servers per group
  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]
  sa := make([][]*ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      sa[i][j] = StartServer(gids[i], smh, ha[i], j, loadBalance)
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func() { cleanup(sa) ; mcleanup(sma) }
  return smh, gids, ha, sa, clean
}

// Benchmark for automatic load balancing
// Test these on regular Paxos to make sure our tests are only
// changing one variable at a time.
func TestReadWrite(t *testing.T) {
  smh, gids, ha, _, clean := setup("basic", false, true)
  defer clean()

  fmt.Printf("Test: Basic write/read...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])
  mck.Join(gids[1], ha[1])

  ck := MakeClerk(smh)

  testString := "hello"
  files := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

  for i:=0; i<4; i++{
    ck.Write(files[i], []byte(testString), false)
  }

  done := make(chan bool, 500)

  // make a, b, c, d, e really popular
  for i:=0; i<500; i++ {
    go func() {
      ck.Write(files[0], []byte(testString), false)
      ck.Write(files[1], []byte(testString), false)
      done <- true
    }()

    go func() {
      ck.Read(files[0], len(testString), 0, false)
      done <- true
    }()

    go func() {
      ck.Read(files[1], len(testString), 0, false)
      done <- true
    }()
  }

  received := 0
  for received != 1500 {
    <- done
    received += 1
  }

  fmt.Printf("  ... Passed\n")
}

func TestBenchmark(t *testing.T) {
  var total float64 = 0
  var divide float64 = 50

  for i:=0; i<50; i++ {
    start := time.Now()
    TestReadWrite(t)
    end := time.Now()
    total += end.Sub(start).Seconds()
  }
  
  avg := total/divide

  fmt.Printf("Average is "+ strconv.FormatFloat(avg, 'f', 4, 64) + "\n")
}
