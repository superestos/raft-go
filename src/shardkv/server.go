package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "6.824/shardctrler"
import "sync"
import "6.824/labgob"
import "bytes"
import "time"

const commitTimeout = 300

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	ClerkId int64
	CommandId int32
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck *shardctrler.Clerk

	persister *raft.Persister

	db map[string]string
	notifyCh map[int](chan int)
	lastCommand map[int64]int32

	index int
	term int
}

type SnapshotInfo struct {
	DB map[string]string
	LastCommand map[int64]int32

	Term int
	Index int
}

func (kv *ShardKV) createSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	s := SnapshotInfo{kv.db, kv.lastCommand, kv.term, kv.index}
	e.Encode(s)
	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.index, snapshot)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1{
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	s := SnapshotInfo{}
	d.Decode(&s)

	if kv.rf.CondInstallSnapshot(s.Term, s.Index, snapshot) {
		if s.DB != nil {
			kv.db = s.DB
		}
		if s.LastCommand != nil {
			kv.lastCommand = s.LastCommand
		}
	
		kv.term = s.Term
		kv.index = s.Index
	}
}

func (kv *ShardKV) start(op Op) bool {
	kv.mu.Lock()
	index, term, ok := kv.rf.Start(op)

	if ok {
		ch := make(chan int, 1)
		kv.notifyCh[index] = ch
		kv.mu.Unlock()

		defer func() {
			kv.mu.Lock()
			delete(kv.notifyCh, index)
			kv.mu.Unlock()
		}()

		select {
		case commandTerm := <-ch:
			return commandTerm == term
		case <-time.After(commitTimeout * time.Millisecond):
		}
	} else {
		kv.mu.Unlock()
	}

	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{args.Key, "", "Get", args.ClerkId, args.CommandId}

	if kv.start(op) {
		kv.mu.Lock()
		value, existed := kv.db[args.Key]
		kv.mu.Unlock()

		if existed {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Key, args.Value, args.Op, args.ClerkId, args.CommandId}

	if kv.start(op) {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) isDuplicate(clerkId int64, commandId int32) bool {
	lastCommand, existed := kv.lastCommand[clerkId]
	if !existed {
		kv.lastCommand[clerkId] = 0
		lastCommand = 0
	}
	return lastCommand >= commandId
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.persister = persister

	kv.db = make(map[string]string)
	kv.notifyCh = make(map[int](chan int))
	kv.lastCommand = make(map[int64]int32)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	kv.readSnapshot(snapshot)

	go kv.applyStateMachine()

	return kv
}

func (kv *ShardKV) applyStateMachine() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()
			if !kv.isDuplicate(op.ClerkId, op.CommandId) {
				if op.Op == "Put" {
					kv.db[op.Key] = op.Value
				} else if op.Op == "Append" {
					kv.db[op.Key] += op.Value
				}
				kv.lastCommand[op.ClerkId] = op.CommandId
			}
			
			kv.index = msg.CommandIndex
			kv.term = msg.CommandTerm

			if kv.notifyCh[msg.CommandIndex] != nil {
				kv.notifyCh[msg.CommandIndex] <- msg.CommandTerm
			}

			if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
				kv.createSnapshot()
			}

			kv.mu.Unlock()

		} else if msg.SnapshotValid {
			snapshot := msg.Snapshot

			kv.mu.Lock()
			kv.readSnapshot(snapshot)
			kv.mu.Unlock()
		}
	}
}