package shardmaster

import "bufio"
import "encoding/json"
import "log"
import "os"
import "strconv"

// JSON marshaling does not support map keys that are not strings
// so we create a string version that can be marshaled
type JSONConfig struct {
  Num int
  Shards [NShards]int64
  Groups map[string][]string
}

// clears the log files by first deleting them (if they exist)
// and then recreating them
func (sm *ShardMaster) clearFiles(){
	os.Remove(sm.configFile)

	f, err := os.OpenFile(sm.configFile, os.O_WRONLY|os.O_CREATE, 0666)

	if err != nil {
		log.Fatal(err)
		return
	}

	f.Close()
}

// writes the given config at the end of configFile in JSON form
func (sm *ShardMaster) writeConfig(config Config) {
	toWrite := JSONConfig{Num: config.Num, Shards: config.Shards}
	newGroups := make(map[string][]string)
	for gid, vals := range config.Groups {
		newGroups[strconv.FormatInt(gid, 10)] = vals
	}
	toWrite.Groups = newGroups
	b, err := json.Marshal(toWrite)
	if err != nil {
		log.Fatal(err)
	}
	f, err2 := os.OpenFile(sm.configFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		log.Fatal(err)
	}
	defer f.Close()
	_, err = f.Write(b)
	_, err = f.WriteString("\n")
}

// makes the sm.configs by reading from configFile
func (sm *ShardMaster) makeConfig(){
	f, err := os.OpenFile(sm.configFile, os.O_RDONLY, 0666)
	if os.IsNotExist(err){
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var config JSONConfig
		err := json.Unmarshal(scanner.Bytes(), &config)
		if err != nil {
			return
		}
		sm.addToConfigs(config)
	}
}

// adds the given JSONConfig to the end of sm.configs by recreating the Config
// and appending it to the end of sm.Configs
func (sm *ShardMaster) addToConfigs(json JSONConfig){
	var config Config = Config{Num: json.Num, Shards: json.Shards}
	grps := make(map[int64][]string)

	for grpString, vals := range json.Groups {
		gid, _ := strconv.ParseInt(grpString, 10, 64)
		grps[gid] = vals
	}

	config.Groups = grps
	sm.configs = append(sm.configs, config)
}