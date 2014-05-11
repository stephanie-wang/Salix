package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
import "path"
import "time"
import "fmt"
//import "sync"
import "math/rand"

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

func NextValue(hprev string, val string) string {
  h := hash(hprev + val)
  return strconv.Itoa(int(h))
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

func setup(tag string, unreliable bool) ([]string, []int64, [][]string, [][]*ShardKV, func()) {
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
      sa[i][j] = StartServer(gids[i], smh, ha[i], j, true)
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func() { cleanup(sa) ; mcleanup(sma) }
  return smh, gids, ha, sa, clean
}

func TestReadWrite(t *testing.T) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Basic write/read...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)

  testString := "hello"
  testFile := "testRead"
  ck.Write(testFile, []byte(testString), false)
  out := ck.Read(testFile, len(testString), 0, false)

  if string(out) != testString {
    t.Fatalf("expected %s, got %s", testString, out)
  }

  fmt.Printf("  ... Passed\n")
}

func TestTransferShard(t *testing.T) {
  smh, gids, ha, _, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Transfer shard...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)

  testString := "hello"
  ck.Write("testTransfer", []byte(testString), false)
  out := ck.Read("testTransfer", len(testString), 0, false)

  if string(out) != testString {
    t.Fatalf("expected %s from initial read, got %s", testString, out)
  }

  mck.Join(gids[1], ha[1])
  mck.Move(key2shard("testTransfer"), gids[1])
  time.Sleep(3 * time.Second)

  out = ck.Read("testTransfer", len(testString), 0, false)
  if string(out) != testString {
    t.Fatalf("expected %s from read after move, got %s", testString, out)
  }

  var count int
  for _, addr := range ha[1] {
    fTmp, _ := os.Open(path.Join(addr + "-root", "tmp", "3", "testTransfer"))
    outTmp := make([]byte, len(testString))
    fTmp.Read(outTmp)
    fTmp.Close()

    fRoot, _ := os.Open(path.Join(addr + "-root", "testTransfer"))
    outRoot := make([]byte, len(testString))
    fRoot.Read(outRoot)
    fRoot.Close()

    if string(outTmp) == testString || string(outRoot) == testString {
      count++
    }
  }
  if count <= len(ha[1])/2 {
    t.Fatalf("file was not transferred to majority of group %d", gids[1])
  }


  fmt.Printf("  ... Passed\n")
}

func TestLimp(t *testing.T) {
  smh, gids, ha, sa, clean := setup("limp", false)
  defer clean()

  fmt.Printf("Test: Reconfiguration with some dead replicas ...\n")

  mck := shardmaster.MakeClerk(smh)
  mck.Join(gids[0], ha[0])

  ck := MakeClerk(smh)

  testString := "hello"
  ck.Write("a", []byte(testString), false)
  out := ck.Read("a", len(testString), 0, false)

  if string(out) != testString {
    t.Fatalf("expected %s from initial read, got %s", testString, out)
  }

  for g := 0; g < len(sa); g++ {
    sa[g][rand.Int() % len(sa[g])].Kill()
  }

  keys := make([]string, 10)
  vals := make([]string, len(keys))
  for i := 0; i < len(keys); i++ {
    keys[i] = strconv.Itoa(rand.Int())
    vals[i] = strconv.Itoa(rand.Int())
    ck.Write(keys[i], []byte(vals[i]), false)
    //ck.Put(keys[i], vals[i])
  }

  // are keys still there after joins?
  for g := 1; g < len(gids); g++ {
    mck.Join(gids[g], ha[g])
    time.Sleep(1 * time.Second)
    for i := 0; i < len(keys); i++ {
      //v := ck.Get(keys[i])
      v := string(ck.Read(keys[i], -1, 0, false))
      if v != vals[i] {
        t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      //ck.Put(keys[i], vals[i])
      ck.Write(keys[i], []byte(vals[i]), false)
    }
  }

  // are keys still there after leaves?
  for g := 0; g < len(gids)-1; g++ {
    mck.Leave(gids[g])
    time.Sleep(2 * time.Second)
    for i := 0; i < len(sa[g]); i++ {
      sa[g][i].Kill()
    }
    for i := 0; i < len(keys); i++ {
      //v := ck.Get(keys[i])
      v := string(ck.Read(keys[i], -1, 0, false))
      if v != vals[i] {
        t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
          g, keys[i], vals[i], v)
      }
      vals[i] = strconv.Itoa(rand.Int())
      //ck.Put(keys[i], vals[i])
      ck.Write(keys[i], []byte(vals[i]), false)
    }
  }

  fmt.Printf("  ... Passed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
  smh, gids, ha, _, clean := setup("conc"+strconv.FormatBool(unreliable), unreliable)
  defer clean()

  mck := shardmaster.MakeClerk(smh)
  for i := 0; i < len(gids); i++ {
    mck.Join(gids[i], ha[i])
  }

  const npara = 11
  var ca [npara]chan bool
  for i := 0; i < npara; i++ {
    ca[i] = make(chan bool)
    go func(me int) {
      ok := true
      defer func() { ca[me] <- ok }()
      ck := MakeClerk(smh)
      mymck := shardmaster.MakeClerk(smh)
      key := strconv.Itoa(me)
      last := ""
      for iters := 0; iters < 3; iters++ {
        nv := strconv.Itoa(rand.Int())
        v := ck.WriteHash(key, []byte(nv), false)
        if string(v) != last {
          ok = false
          t.Fatalf("WriteHash(%v) expected %v got %v\n", key, last, v)
        }
        last = NextValue(last, nv)
        v = ck.Read(key, -1, 0, false)
        if string(v) != last {
          ok = false
          t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
        }

        mymck.Move(rand.Int() % shardmaster.NShards,
          gids[rand.Int() % len(gids)])

        time.Sleep(time.Duration(rand.Int() % 30) * time.Millisecond)
      }
    }(i)
  }

  for i := 0; i < npara; i++ {
    x := <- ca[i]
    if x == false {
      t.Fatalf("something is wrong")
    }
  }
}

func TestConcurrent(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move ...\n")
  doConcurrent(t, false)
  fmt.Printf("  ... Passed\n")
}

func TestConcurrentUnreliable(t *testing.T) {
  fmt.Printf("Test: Concurrent Put/Get/Move (unreliable) ...\n")
  doConcurrent(t, true)
  fmt.Printf("  ... Passed\n")
}
