package main

import "bufio"
import "flag"
import "os"
import "runtime"
import "strings"
import "strconv"
import "shardkv"
import "shardmaster"

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

func cleanup(sa [][]*shardkv.ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].Kill()
    }
  }
}

func setup(tag string, nmasters int, ngroups int, nreplicas int) ([]string, []int64, [][]string, [][]*shardkv.ShardKV, func()) {
  runtime.GOMAXPROCS(4)

  //const nmasters = 3
  var sma []*shardmaster.ShardMaster = make([]*shardmaster.ShardMaster, nmasters)
  var smh []string = make([]string, nmasters)
  // defer mcleanup(sma)
  for i := 0; i < nmasters; i++ {
    smh[i] = port(tag+"m", i)
  }
  for i := 0; i < nmasters; i++ {
    sma[i] = shardmaster.StartServer(smh, i)
  }

  //const ngroups = 2   // replica groups
  //const nreplicas = 3 // servers per group
  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]
  sa := make([][]*shardkv.ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*shardkv.ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      sa[i][j] = shardkv.StartServer(gids[i], smh, ha[i], j, true)
    }
  }

  clean := func() { cleanup(sa) ; mcleanup(sma) }
  return smh, gids, ha, sa, clean
}

func makeShardkvClerk(filename string, smhArgs string) *shardkv.Clerk {
  if filename == "" {
    return nil
  }

  smh := strings.FieldsFunc(smhArgs, func(r rune) bool {
    return r == ','
  })
  if len(smh) == 0 {
    return nil
  }
  return shardkv.MakeClerk(smh)
}

func makeShardmasterClerk(smhArgs string) *shardmaster.Clerk {
  smh := strings.FieldsFunc(smhArgs, func(r rune) bool {
    return r == ','
  })
  println(smh)
  return shardmaster.MakeClerk(smh)
}

// command-line args
var command = flag.String("command", "Start",
  "Command can be Start, Exit, Read, Write, Remove")

var filename = flag.String("filename", "",
  "Provide a filename to read, write, remove")

// start args
var nmasters = flag.Int("nmasters", 3,
  "Number of shardmaster peers")
var ngroups = flag.Int("ngroups", 2,
  "Number of shardkv replica groups")
var nreplicas = flag.Int("nreplicas", 3,
  "Number of replicas per shardkv group")

// required for all commands other than start
var smh = flag.String("smh", "",
  "Shardmaster hosts, comma-separated please :)")

// read args
var bytes = flag.Int("bytes", -1,
  "In read command, number of bytes to read, or none to read whole file")
var offset = flag.Int("offset", 0,
  "In read command, offset to start reading from")
var stale = flag.Bool("stale", false,
  "In read command, set to true to do a stale read")

// write args
var contents = flag.String("contents", "",
  "In write command, the contents to write to file")
var doAppend = flag.Bool("doAppend", false,
  "In write command, set to true to append instead of overwriting a file")
var doHash = flag.Bool("doHash", false,
  "In write command, set to true to write hashed of previous value")

// shardmaster join/leave args 
var gid = flag.Int64("gid", 0,
  "In join/leave command, set to the gid of the server group")
var ha = flag.String("ha", "",
  "In join command, set to list of replica addresses for server group, comma-separated")

func main() {
  flag.Parse()

  switch *command {
  case "Start":
    r := bufio.NewReader(os.Stdin)
    smh, gids, ha, _, clean := setup("basic", *nmasters, *ngroups, *nreplicas)
    //smh := []string{"hi", "hello"}
    //gids := []int64{0, 0}
    //ha := make([][]string, 2)
    //ha[0] = []string{"poop", "poop"}
    //ha[1] = []string{"poopy", "poop"}
    defer clean()

    println("smh: " + strings.Join(smh, ","))
    for i, gid := range gids {
      println(strconv.Itoa(int(gid)))
      println("ha: " + strings.Join(ha[i], ","))
    }

    for {
      line, _ := r.ReadString('\n')
      if line != "Exit\n" {
        continue
      } else {
        return
      }
    }
  case shardkv.Read:
    ck := makeShardkvClerk(*filename, *smh)
    if ck == nil {
      println("must specify filename and shardmaster hosts")
      return
    }
    v := ck.Read(*filename, *bytes, int64(*offset), *stale)
    println(string(v))
  case shardkv.Write:
    ck := makeShardkvClerk(*filename, *smh)
    if ck == nil {
      println("must specify filename and shardmaster hosts")
      return
    }
    if *doHash {
      ck.WriteHash(*filename, []byte(*contents), *doAppend)
    } else {
      ck.Write(*filename, []byte(*contents), *doAppend)
    }
    v := ck.Read(*filename, *bytes, int64(*offset), *stale)
    println(string(v))
  case shardkv.Remove:
    ck := makeShardkvClerk(*filename, *smh)
    if ck == nil {
      println("must specify filename and shardmaster hosts")
      return
    }
    ck.Remove(*filename)

  case shardmaster.Join:
    println("HI")
    ck := makeShardmasterClerk(*smh)
    println("HI")
    has := strings.FieldsFunc(*ha, func(r rune) bool {
      return r == ','
    })
    ck.Join(*gid, has)
  case shardmaster.Leave:
    ck := makeShardmasterClerk(*smh)
    ck.Leave(*gid)
  }
}
