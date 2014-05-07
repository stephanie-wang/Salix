package shardmaster

import "fmt"
import "os"
import "strconv"
import "strings"

// writes the given string to the file
func (sm *ShardMaster) writeConfig(config Config){
	f, err := os.OpenFile(sm.configFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	if err != nil  {
		fmt.Print(err)
		return
	}

	defer f.Close()

	_, err = f.WriteString(sm.encodeConfig(config))
	if err != nil {
		fmt.Println(err)
	}
}

// clears the log files by first deleting them (if they exist)
// and then recreating them
func (sm *ShardMaster) clearFiles(){
	os.Remove(sm.configFile)

	f, err := os.OpenFile(sm.configFile, os.O_WRONLY|os.O_CREATE, 0666)

	if err != nil {
		fmt.Print(err)
		return
	}

	f.Close()
}


// makes a configuration by reading the config file and decoding the configs
// adds them to the shardmaster's configuration
func (sm *ShardMaster) makeConfig() {
}

// encodes a given config to string form
// the form used is $<config num>;<comma-separated group num>;
// <comma-separated servers by group>|*;<comma-separated shard num>$\n
// example:
// "$2;1,2,3;a,b,c|d,e,f|g,h,i;1,1,2,2,2,3,3,3,1,1$\n""
func (sm *ShardMaster) encodeConfig(config Config) string {
	configString := "$"+strconv.Itoa(config.Num)

	groups := make([]string, len(config.Groups))
	servers := make([]string, len(config.Groups))

	i := 0
	for grp, srvs := range config.Groups {
		groups[i] = strconv.FormatInt(grp, 10)
		servers[i] = strings.Join(srvs, ",")
		i++
	}

	groupString := strings.Join(groups, ",")
	serverString := strings.Join(servers, "|")

	temp := make([]string, len(config.Shards))
	for i, shard := range config.Shards{
		temp[i] = strconv.FormatInt(shard, 10)
	}

	shardString := strings.Join(temp, ",")

	total := []string{configString, groupString, serverString, shardString, "$\n"}

	return strings.Join(total, ";")
}

// decodes a configuraiton from string from
// returns the Configuration and true, if it was successfully decoded
// or returns an empty Configuraiton and false if there was a problem in decoding
// this will happen if the machine goes down before it's done writing the config to disk
func (sm *ShardMaster) decodeConfig(text string) (Config, bool) {
	if strings.Count(text, "$") != 2 {
		return Config{}, false
	}
	
	text = text[1:len(text)-3]
	comp := strings.Split(text, ";")
	
	if len(comp) != 4 {
		return Config{}, false
	}

	num, _ := strconv.ParseInt(comp[0], 10, 0)
	return Config{Num: int(num), Groups: sm.parseGroups(comp[1], comp[2]), Shards: sm.parseShards(comp[3])}, true
}

// makes a map of the groups and the corresponding servers given a string
// that represents the group id's (comma separated), and a string that reprsentes server id's
// per group (comma separated by group, separated by a vertical line between groups)
func (sm *ShardMaster) parseGroups(groupString string, serverString string) map[int64][]string {
	out := make(map[int64][]string)
	groups := strings.Split(groupString, ",")
	servers := strings.Split(serverString, "|")

	for i, group := range groups {
		serverList := strings.Split(servers[i], ",")
		gid, _ := strconv.ParseInt(group, 10, 64)
		out[gid] = serverList
	}
	return out
}

// makes a list of the shards given a comma-separated string that represents these shards
func (sm *ShardMaster) parseShards(text string) [NShards]int64{
	shards := strings.Split(text, ",")
	var out [NShards]int64

	for i, shard := range shards {
		val, _ := strconv.ParseInt(shard, 10, 64)
		out[i] = val
	}

	return out
}