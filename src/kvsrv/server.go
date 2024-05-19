package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data              map[string]string
	processedRequests map[string]string
}

func (kv *KVServer) hasAlreadyProcessedRequestSavingOldValue(idempotencyKey string, oldValue string) bool {
	_, ok := kv.processedRequests[idempotencyKey]
	if ok {
		return true
	}
	kv.processedRequests[idempotencyKey] = oldValue
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.hasAlreadyProcessedRequestSavingOldValue(args.IdempotencyKey, "") { // no return, so no need for oldValue.
		return
	}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.data[args.Key]
	if kv.hasAlreadyProcessedRequestSavingOldValue(args.IdempotencyKey, oldValue) {
		reply.Value = kv.processedRequests[args.IdempotencyKey]
		return
	}

	kv.data[args.Key] += args.Value

	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.processedRequests = make(map[string]string)

	return kv
}
