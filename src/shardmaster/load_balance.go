package shardmaster

// 
// This file contains all the functionality for load-balancing the groups
// The goal is to redistribute the shards to the groups such that the sum of popularity scores
// for all shards served by the group are the same. 
// This algorithm minimizes the difference between the group with the highest and lowest popularity
// score. It stops if there is no optimal way to move from the most to the least popular group.
//
// The algorithm doesn't explicitly optimize for least number of moves. But if all shards have similar
// scores, then # of moves are also minimized. Otherwise, we are guaranteed that the most popular shards
// (the ones with the highest incoming requests) will not be moved around. This means that
// downtime for these files can be minimized.
//

// structure used to keep track of the shard internally
type Shard struct {
	Num int
	Score int
	Group int64
}

func equals(a [NShards]int64, b [NShards]int64) bool {
	for i, gid := range a {
		if gid != b[i] {
			return false
		}
	}
	return true
}

// main load balancing function
// takes in the following arguments:
// 	scores: the popularity score by shard
//	old: the latest configuration's shard to group assignment
//  newGroups: a map (behaving as a set) that contains, as keys, the id's of the new groups
// returns the shard to group assignment for the new configuration after load balancing
func loadBalance(scores [NShards]int, old [NShards]int64, newGroups map[int64]bool) [NShards]int64 {
	groupScore := make(map[int64]int) // gid (of newGroups) --> score

	for gid, _ := range newGroups {
		groupScore[gid] = 0
	}

	// create the shard values
	shards := make([]Shard, NShards)

	for i, gid := range old {
		shards[i] = Shard{Num: i, Score: scores[i], Group: gid}
		_, ok := newGroups[gid]
		if ok {
			groupScore[gid] += scores[i]
		}
	}

	// first assign the shards whose old group is no longer valid
	// i.e. old group is 0 or old group has left
	for i, shard := range shards {
		_, ok := newGroups[shard.Group]
		if !ok {
			newGrp := minGID(groupScore)
			shard.Group = newGrp
			shards[i] = shard
			groupScore[newGrp] += shard.Score
		}
	}

	// now keep taking the lowest scoring shard from the most popular group
	// and put it in the least popular group
	// continue until it no longer makes sense to do this
	for {
		fromGrp := maxGID(groupScore)
		toGrp := minGID(groupScore)
		shard := minShard(shards, fromGrp)

		beforeDiff := groupScore[fromGrp] - groupScore[toGrp]
		if beforeDiff < TChange {
			break
		}

		if shard.Score >= beforeDiff {
			break
		}

		shard.Group = toGrp
		shards[shard.Num] = shard

		groupScore[toGrp] += shard.Score
		groupScore[fromGrp] -= shard.Score
	}

	// create the new configuration in the correct format
	var newConfig [NShards]int64
	for _, shard := range shards {
		newConfig[shard.Num] = shard.Group
	}
	return newConfig

}

// returns the GID of the group in groupScore that has the lowest sum of scores (popularity)
func minGID(groupScore map[int64]int) int64 {
	var gid int64 = 0
	var min int
	
	for group, score := range groupScore {
		if gid == 0 {
			gid = group
			min = score
			continue
		}
		if score <= min {
			min = score
			gid = group
		}
	}
	return gid
}

// returns the GID of the group in groupScore that has the highest sum of scores (popularity)
func maxGID(groupScore map[int64]int) int64 {
	var gid int64
	max := -1

	for group, score := range groupScore {
		if score > max {
			max = score
			gid = group
		}
	}

	return gid
}

// returns the least popular shard in shards that is also assigned to group gid
func minShard(shards []Shard, gid int64) Shard {
	var out Shard
	var min int = -1

	for _, shard := range shards {
		if shard.Group == gid && min == -1 {
			out = shard
			min = shard.Score
			continue
		}
		if shard.Group == gid && shard.Score < min {
			out = shard
			min = shard.Score
		}
	}
	return out
}
