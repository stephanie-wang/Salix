package shardkv

import "bufio"
import "encoding/json"
//import "io"
import "io/ioutil"
import "os"
import "strconv"
import "strings"
import "shardmaster"

func appendToLog(logFile string, line []string) {
  f, _ := os.OpenFile(logFile, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0666)
  _, err := f.Write([]byte(strings.Join(line, " ") + "\n"))
  if err != nil {
    println(err.Error())
  }
  f.Close()
}

func appendConfig(logFile string, config *shardmaster.Config) {
  configJson, _ := json.Marshal(config)
  appendToLog(logFile, []string{string(configJson)})
}

func getConfig(logFile string, configNum int) *shardmaster.Config {
  log, _ := os.Open(logFile)
  r := bufio.NewReader(log)
  var configString string
  for i := 0; i <= configNum; i++ {
    configString, _ = r.ReadString('\n')
  }
  var config *shardmaster.Config
  json.Unmarshal([]byte(configString), config)
  return config
}

func logLiterals(logFile string, key string, val int) {
  literalsJson, _ := ioutil.ReadFile(logFile)
  literals := make(map[string]int)
  json.Unmarshal(literalsJson, &literals)
  literals[key] = val
  literalsJson, _ = json.Marshal(literals)
  ioutil.WriteFile(logFile, literalsJson, 0666)
}

func recoverLiterals(logFile string) map[string]int {
  literalsJson, _ := ioutil.ReadFile(logFile)
  literals := make(map[string]int)
  json.Unmarshal(literalsJson, &literals)
  return literals
}

func logStore(logFile string, shard int, files []string) {
  jsonFiles, _ := json.Marshal(files)
  line := []string{"store",
    strconv.Itoa(shard),
    string(jsonFiles),
  }
  appendToLog(logFile, line)
}

func logSeen(logFile string, shard int, id int64, reply *Reply) {
  replyJson, _ := json.Marshal(reply)
  appendToLog(logFile, []string{"seen",
    strconv.Itoa(shard),
    strconv.Itoa(int(id)),
    string(replyJson)})
}

func logReconfig(logFile string, configNum int, seq int) {
  appendToLog(logFile, []string{"reconfig", strconv.Itoa(configNum), strconv.Itoa(seq)})
}

func logShardsToTransfer(logFile string, configNum int, gid int64, args *ReshardArgs) {
  argsJson, _ := json.Marshal(args)
  line := append([]string{"shardsToTransfer",
      strconv.Itoa(configNum),
      strconv.Itoa(int(gid))},
      string(argsJson))
  appendToLog(logFile, line)
}

func getLogIndex(logFile string) int {
  f, err := os.Open(logFile)
  if err != nil {
    return -1
  }
  r := bufio.NewReader(f)
  var i int
  for err == nil {
    _, err = r.ReadString('\n')
    i++
  }
  return i
}

func recoverMaps(logFile string, checkpoint int) (store map[int][]string,
    seen map[int]map[int64]*Reply,
    reconfigs map[int]int,
    shardsToTransfer map[int]map[int64]*ReshardArgs) {

  store = make(map[int][]string)
  seen = make(map[int]map[int64]*Reply)
  reconfigs = make(map[int]int)
  shardsToTransfer = make(map[int]map[int64]*ReshardArgs)

  stringStore := make(map[string][]string)
  stringSeen := make(map[string]map[string]*Reply)
  stringReconfigs := make(map[string]int)
  stringShards := make(map[string]map[string]*ReshardArgs)

  checkpointFile, _ := os.Open(logFile + "-checkpoint")
  r := bufio.NewReader(checkpointFile)
  jsonString, _ := r.ReadString('\n')
  err := json.Unmarshal([]byte(jsonString), &stringStore)
  if err != nil {
    println(err.Error())
  }
  json.Unmarshal([]byte(jsonString), &stringSeen)
  jsonString, _ = r.ReadString('\n')
  json.Unmarshal([]byte(jsonString), &stringReconfigs)
  jsonString, _ = r.ReadString('\n')
  json.Unmarshal([]byte(jsonString), &stringShards)

  for key, vals := range stringStore {
    keyInt, _ := strconv.Atoi(key)
    store[keyInt] = vals
  }
  for key, vals := range stringSeen {
    keyInt, _ := strconv.Atoi(key)
    seen[keyInt] = make(map[int64]*Reply)
    for reqKey, reply := range vals {
      reqKeyInt, _ := strconv.Atoi(reqKey)
      seen[keyInt][int64(reqKeyInt)] = reply
    }
  }
  for key, val := range stringReconfigs {
    keyInt, _ := strconv.Atoi(key)
    reconfigs[keyInt] = val
  }
  for key, vals := range stringShards {
    keyInt, _ := strconv.Atoi(key)
    shardsToTransfer[keyInt] = make(map[int64]*ReshardArgs)
    for gid, args := range vals {
      gidInt, _ := strconv.Atoi(gid)
      shardsToTransfer[keyInt][int64(gidInt)] = args
    }
  }

  f, _ := os.Open(logFile)
  w := bufio.NewReader(f)
  for i := 0; i <= checkpoint; i++ {
    w.ReadString('\n')
  }

  fTruncated, _ := os.Create(logFile + "-tmp")
  for {
    line, err := w.ReadString('\n')
    if err != nil {
      break
    }
    fTruncated.Write([]byte(line))

    op := strings.Fields(line)
    switch op[0] {
    case "store":
      if len(op) != 3 {
        break
      }
      shard, _ := strconv.Atoi(op[1])
      var files []string
      json.Unmarshal([]byte(op[2]), &files)
      store[shard] = files
    case "seen":
      if len(op) != 4 {
        break
      }
      shard, _ := strconv.Atoi(op[1])
      req, _ := strconv.Atoi(op[2])
      var reply *Reply
      json.Unmarshal([]byte(op[3]), reply)
      if _, inMap := seen[shard]; !inMap {
        seen[shard] = make(map[int64]*Reply)
      }
      seen[shard][int64(req)] = reply
    case "reconfigs":
      if len(op) != 3 {
        break
      }
      config, _ := strconv.Atoi(op[1])
      seq, _ := strconv.Atoi(op[2])
      reconfigs[config] = seq
    case "shardsToTransfer":
      config, _ := strconv.Atoi(op[1])
      gid, _ := strconv.Atoi(op[2])
      var args *ReshardArgs
      json.Unmarshal([]byte(op[3]), args)
      if _, inMap := shardsToTransfer[config]; !inMap {
        shardsToTransfer[config] = make(map[int64]*ReshardArgs)
      }
      if args == nil {
        delete(shardsToTransfer[config], int64(gid))
      } else {
        shardsToTransfer[config][int64(gid)] = args
      }
    }
  }
  f.Close()
  fTruncated.Close()

  os.Rename(logFile + "-tmp", logFile)

  return
}

func doCheckpoint(logFile string,
    store map[int][]string,
    seen map[int]map[int64]*Reply,
    reconfigs map[int]int,
    shardsToTransfer map[int]map[int64]*ReshardArgs) int {

  stringStore := make(map[string][]string)
  for i, strings := range store {
    stringStore[strconv.Itoa(i)] = strings
  }
  stringSeen := make(map[string]map[string]*Reply)
  for i, replyMap := range seen {
    stringSeen[strconv.Itoa(i)] = make(map[string]*Reply)
    for j, reply := range replyMap {
      stringSeen[strconv.Itoa(i)][strconv.Itoa(int(j))] = reply
    }
  }
  stringReconfigs := make(map[string]string)
  for i, reconfig := range reconfigs {
    stringReconfigs[strconv.Itoa(i)] = strconv.Itoa(reconfig)
  }
  stringShards := make(map[string]map[string]*ReshardArgs)
  for i, argsMap := range shardsToTransfer {
    stringShards[strconv.Itoa(i)] = make(map[string]*ReshardArgs)
    for j, args := range argsMap {
      stringShards[strconv.Itoa(i)][strconv.Itoa(int(j))] = args
    }
  }


  f, _ := os.Create(logFile + "-checkpoint")
  var jsonBytes []byte
  jsonBytes, _ = json.Marshal(stringStore)
  f.Write(jsonBytes)
  f.Write([]byte{'\n'})
  jsonBytes, _ = json.Marshal(stringSeen)
  f.Write(jsonBytes)
  f.Write([]byte{'\n'})
  jsonBytes, _ = json.Marshal(stringReconfigs)
  f.Write(jsonBytes)
  f.Write([]byte{'\n'})
  jsonBytes, _ = json.Marshal(stringShards)
  f.Write(jsonBytes)
  f.Write([]byte{'\n'})
  f.Close()


  return getLogIndex(logFile)
}
