package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string
	notifyCh map[int](chan int)
	lastCommand map[int64]int32
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{args.Key, "", "Get"}
	index, _, ok := kv.rf.Start(op)

	if ok {
		kv.mu.Lock()
		kv.notifyCh[index] = make(chan int, 1)
		kv.mu.Unlock()
		defer delete(kv.notifyCh, index)

		select {
		case <-kv.notifyCh[index]:
			kv.mu.Lock()
			value, existed := kv.db[args.Key]
			kv.mu.Unlock()

			if existed {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
			return

		case <-time.After(1000 * time.Millisecond):
		}
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Key, args.Value, args.Op}
	index, _, ok := kv.rf.Start(op)

	if ok {
		kv.mu.Lock()
		kv.notifyCh[index] = make(chan int, 1)
		kv.mu.Unlock()
		defer delete(kv.notifyCh, index)

		select {
		case <-kv.notifyCh[index]:
			reply.Err = OK
			return
			
		case <-time.After(1000 * time.Millisecond):
		}
	}
	reply.Err = ErrWrongLeader
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.notifyCh = make(map[int](chan int))
	kv.lastCommand = make(map[int64]int32)

	go kv.applyStateMachine()

	return kv
}

func (kv *KVServer) applyStateMachine() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			op := msg.Command.(Op)
			if op.Op == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.Op == "Append" {
				kv.db[op.Key] += op.Value
			}

			kv.notifyCh[msg.CommandIndex] <- 0
		}
	}
}
