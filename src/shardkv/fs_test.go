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
//import "math/rand"

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

//func NextValue(hprev string, val string) string {
//  h := hash(hprev + val)
//  return strconv.Itoa(int(h))
//}

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
      sa[i][j].kill()
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
  const nreplicas = 1 // servers per group
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
      sa[i][j] = StartServer(gids[i], smh, ha[i], j)
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
  ck.Write(testFile, []byte(testString))
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
  ck.Write("testTransfer", []byte(testString))
  out := ck.Read("testTransfer", len(testString), 0, false)

  if string(out) != testString {
    t.Fatalf("expected %s, got %s", testString, out)
  }

  mck.Join(gids[1], ha[1])
  mck.Move(key2shard("testTransfer"), gids[1])
  time.Sleep(3 * time.Second)

  f, _ := os.Open(path.Join(ha[1][0] + "-root", "testTransfer"))
  out = make([]byte, len(testString))
  f.Read(out)
  if string(out) != testString {
    t.Fatalf("expected %s, got %s", testString, out)
  }

  fmt.Printf("  ... Passed\n")
}
