package shardmaster

//
// This package includes functions that read to and write from disk.
// They are used for the server's write-ahead-log, so that upon recovery after a
// failure, the server may recover its memory contents by reading from disk.
// For a given server with id x, it creates 2 files
// 		sm-config-x: stores all the configurations x has applied since the beginning
//					 stores them in JSON format, where configurations are
//					 separated by a newline
//					 the writer appends to this file every new configuration
//		sm-score-x:  stores both the scores that were heard and the groups that
//					 have announced the scores for the most recent configuration.
// 					 this is stored in a single JSON object
//					 this file is re-written every time this changes
//

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

type JSONScore struct {
	Scores [NShards]int
	Heard map[string]int // string(gid) --> highest sequence number
}

// clears the log files by first deleting them (if they exist)
// and then recreating them
func (sm *ShardMaster) clearFiles(){
	os.Remove(sm.configFile)
	os.Remove(sm.scoreFile)

	f, err := os.OpenFile(sm.configFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
		return
	}
	f.Close()

	var scores [NShards]int
	for i:=0; i<NShards; i++{
		scores[i] = 1
	}
	
	newHeard := make(map[string]int)
	toWrite := JSONScore{Scores: scores, Heard: newHeard}
	b, _ := json.Marshal(toWrite)

	f, err = os.OpenFile(sm.scoreFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
		return
	}
	_, err = f.WriteString(string(b) + "\n")
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
		log.Fatal(err2)
	}
	defer f.Close()
	_, err = f.WriteString(string(b) + "\n")
}

// writes the popularity scores and highest sequence # heard per group to file
func (sm *ShardMaster) writeScores(scores [NShards]int, heard map[int64]int){
	toWrite := JSONScore{Scores: scores}
	newHeard := make(map[string]int)
	for gid, val := range heard {
		newHeard[strconv.FormatInt(gid, 10)] = val
	}
	toWrite.Heard = newHeard

	b, err := json.Marshal(toWrite)
	if err != nil {
		log.Fatal(err)
	}
	f, err2 := os.OpenFile(sm.scoreFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		log.Fatal(err2)
	}
	defer f.Close()
	_, err = f.WriteString(string(b))
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
		err = json.Unmarshal(scanner.Bytes(), &config)
		if err != nil {
			return
		}
		sm.addToConfigs(config)
	}
}

// makes sm.scores and sm.latestHeard from the scoreFile
func (sm *ShardMaster) makeScores() {
	f, err := os.OpenFile(sm.scoreFile, os.O_RDONLY, 0666)
	if os.IsNotExist(err){
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var score JSONScore
		err = json.Unmarshal(scanner.Bytes(), &score)
		if err != nil {
			return
		}
		sm.addScores(score)
		return
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

// takes the given JSONScore and uses it to populate sm.scores
// and sm.latestHeard
func (sm *ShardMaster) addScores(json JSONScore) {
	sm.scores = json.Scores
	heard := make(map[int64]int)

	for grpString, val := range json.Heard {
		gid, _ := strconv.ParseInt(grpString, 10, 64)
		heard[gid] = val
	}

	sm.latestHeard = heard
}

