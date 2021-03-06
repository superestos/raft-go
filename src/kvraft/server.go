package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const commitTimeout = 300

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
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

func (kv *KVServer) createSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	s := SnapshotInfo{kv.db, kv.lastCommand, kv.term, kv.index}
	e.Encode(s)
	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.index, snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
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

func (kv *KVServer) start(op Op) bool {
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{args.Key, args.Value, args.Op, args.ClerkId, args.CommandId}

	if kv.start(op) {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) isDuplicate(clerkId int64, commandId int32) bool {
	lastCommand, existed := kv.lastCommand[clerkId]
	if !existed {
		kv.lastCommand[clerkId] = 0
		lastCommand = 0
	}
	return lastCommand >= commandId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

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

func (kv *KVServer) applyStateMachine() {
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

			if kv.index + 1 != msg.CommandIndex {
				DPrintf("Warning: server get unexpected msg, %d, %d\n", kv.index + 1, msg.CommandIndex)
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
