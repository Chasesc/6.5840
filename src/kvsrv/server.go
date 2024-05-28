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

type clientRequestData struct {
	oldValue       string
	idempotencyKey string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data              map[string]string
	processedRequests map[int64]clientRequestData
}

func (kv *KVServer) isClientRequestDuplicate(args *PutAppendArgs, oldValue string) bool {
	previousResult, ok := kv.processedRequests[args.ClientID]
	if ok && args.IdempotencyKey == previousResult.idempotencyKey {
		return true
	}

	kv.processedRequests[args.ClientID] = clientRequestData{oldValue: oldValue, idempotencyKey: args.IdempotencyKey}
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
	if kv.isClientRequestDuplicate(args, "") { // no return, so no need for oldValue.
		return
	}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.data[args.Key]
	if kv.isClientRequestDuplicate(args, oldValue) {
		reply.Value = kv.processedRequests[args.ClientID].oldValue
		return
	}

	kv.data[args.Key] += args.Value

	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.processedRequests = make(map[int64]clientRequestData)

	return kv
}
