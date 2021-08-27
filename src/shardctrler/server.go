package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

import "time"

import "fmt"

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
	op.Op = "Leave"
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
// servers that will cooperate via Paxos to
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

func (sc *ShardCtrler) applyStateMachine() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			sc.mu.Lock()
			if !sc.isDuplicate(op.ClerkId, op.CommandId) {
				config := sc.configs[len(sc.configs) - 1]
				config.Num += 1

				if op.Op == "Join" {
					for k, v := range op.Servers {
						config.Groups[k] = v
					}
				} else if op.Op == "Leave" {
					for i := 0; i < len(op.GIDs); i++ {
						delete(config.Groups, op.GIDs[i])
					}
				} else if op.Op == "Move" {
					config.Shards[op.Shard] = op.GID
				}

				if op.Op != "Query" {
					sc.configs[len(sc.configs)] = config
				}

				fmt.Println(config)
				fmt.Println(sc.configs)

				sc.lastCommand[op.ClerkId] = op.CommandId
			}

			if sc.notifyCh[msg.CommandIndex] != nil {
				sc.notifyCh[msg.CommandIndex] <- msg.CommandTerm
			}
			sc.mu.Unlock()
		}
	}
}
