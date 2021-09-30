package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

import "time"
import "sort"

//import "fmt"

const commitTimeout = 300

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	notifyCh map[int](chan int)
	lastCommand map[int64]int32

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.

	//Join
	Servers map[int][]string

	//Leave
	GIDs []int

	//Move
	Shard int
	GID   int

	//Query
	Num int

	Op    string
	ClerkId int64
	CommandId int32
}

func (sc *ShardCtrler) start(op Op) bool {
	sc.mu.Lock()
	index, term, ok := sc.rf.Start(op)

	if ok {
		ch := make(chan int, 1)
		sc.notifyCh[index] = ch
		sc.mu.Unlock()

		defer func() {
			sc.mu.Lock()
			delete(sc.notifyCh, index)
			sc.mu.Unlock()
		}()

		select {
		case commandTerm := <-ch:
			return commandTerm == term
		case <-time.After(commitTimeout * time.Millisecond):
		}
	} else {
		sc.mu.Unlock()
	}

	return false
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.Servers = args.Servers
	op.Op = "Join"
	op.ClerkId = args.ClerkId
	op.CommandId = args.CommandId

	if sc.start(op) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.GIDs = args.GIDs
	op.Op = "Leave"
	op.ClerkId = args.ClerkId
	op.CommandId = args.CommandId

	if sc.start(op) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.Shard = args.Shard
	op.GID = args.GID
	op.Op = "Move"
	op.ClerkId = args.ClerkId
	op.CommandId = args.CommandId

	if sc.start(op) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Num = args.Num
	op.Op = "Query"
	op.ClerkId = args.ClerkId
	op.CommandId = args.CommandId

	if sc.start(op) {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		if op.Num == -1 || op.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs) - 1]
		} else {
			reply.Config = sc.configs[op.Num]
		}
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	sc.notifyCh = make(map[int](chan int))
	sc.lastCommand = make(map[int64]int32)

	go sc.applyStateMachine()

	return sc
}

func (sc *ShardCtrler) isDuplicate(clerkId int64, commandId int32) bool {
	lastCommand, existed := sc.lastCommand[clerkId]
	if !existed {
		sc.lastCommand[clerkId] = 0
		lastCommand = 0
	}
	return lastCommand >= commandId
}

func (sc *ShardCtrler) countGID(config Config) (map[int]int, int) {
	count := 0
	nGID := make(map[int]int)
	for gid, _ := range config.Groups {
		nGID[gid] = 0
	}

	for i := 0; i < NShards; i += 1{
		gid := config.Shards[i]
		n, existed := nGID[gid]
		if existed {
			count += 1
			nGID[gid] = n + 1
		}
	}

	return nGID, count
}

func (sc *ShardCtrler) findMinMaxGID(counts map[int]int, gids []int) (int, int) {
	nGroups := len(gids)

	minCount := NShards + 1
	maxCount := -1
	minGID := 0
	maxGID := 0

	for i := 0; i < nGroups; i += 1 {
		if counts[gids[i]] < minCount {
			minGID = gids[i]
			minCount = counts[gids[i]]
		}
		if counts[gids[i]] > maxCount {
			maxGID = gids[i]
			maxCount = counts[gids[i]]
		}
	}

	return minGID, maxGID
}

func (sc *ShardCtrler) rebalance(config Config) Config {
	nGroups := len(config.Groups)
	if nGroups == 0 {
		return config
	}

	gids := make([]int, nGroups)
	i := 0
	for gid, _ := range config.Groups {
		gids[i] = gid
		i += 1
	}
	sort.Ints(gids)
	for i = 0; i < nGroups / 2; i += 1 {
		temp := gids[i]
		gids[i] = gids[nGroups - 1 - i]
		gids[nGroups - 1 - i] = temp
	}
	
	nGID, count := sc.countGID(config)
	minGID, maxGID := sc.findMinMaxGID(nGID, gids)

	for nGID[minGID] + 1 < nGID[maxGID] || count != NShards {
		for i = 0; i < NShards; i += 1 {
			_, existed := config.Groups[config.Shards[i]]
			if !existed || (count == NShards && config.Shards[i] == maxGID) {
				config.Shards[i] = minGID
				break
			}
		}

		nGID, count = sc.countGID(config)
		minGID, maxGID = sc.findMinMaxGID(nGID, gids)
	}

	return config
}

func (sc *ShardCtrler) applyStateMachine() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			sc.mu.Lock()
			if !sc.isDuplicate(op.ClerkId, op.CommandId) {

				if op.Op != "Query" {

					config := Config{}
					prevConfig := sc.configs[len(sc.configs) - 1]
					config.Num = prevConfig.Num + 1
					config.Groups = make(map[int][]string)

					for shard, gid := range prevConfig.Groups {
						config.Groups[shard] = gid
					}
					for i := 0; i < NShards; i += 1 {
						config.Shards[i] = prevConfig.Shards[i]
					}

					if op.Op == "Join" {
						for shard, gid := range op.Servers {
							config.Groups[shard] = gid
						}
						config = sc.rebalance(config)
					} else if op.Op == "Leave" {
						for i := 0; i < len(op.GIDs); i++ {
							if _, existed := config.Groups[op.GIDs[i]]; existed {
								delete(config.Groups, op.GIDs[i])
							}
						}
						config = sc.rebalance(config)
					} else if op.Op == "Move" {
						config.Shards[op.Shard] = op.GID
					}

					sc.configs = append(sc.configs, config)
				}

				sc.lastCommand[op.ClerkId] = op.CommandId
			}

			if sc.notifyCh[msg.CommandIndex] != nil {
				sc.notifyCh[msg.CommandIndex] <- msg.CommandTerm
			}
			sc.mu.Unlock()
		}
	}
}
