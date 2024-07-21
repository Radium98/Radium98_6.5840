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

type PrevEntry struct {
	RpcId    int64
	PreValue string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Dict        map[string]string
	AnyChan     chan ServerTask
	PrevEntries map[int64]PrevEntry
}

type ServerTask struct {
	TaskType string
	PutAppendargs *PutAppendArgs
	PutAppendreply *PutAppendReply
	Getargs *GetArgs
	Getreply *GetReply
}

func (kv *KVServer) checkDuplicate(ClientId, RpcId int64) (bool, string) {
	prev, ok := kv.PrevEntries[ClientId]
	if !ok || prev.RpcId != RpcId {
		return false, ""
	} else {
		return true, prev.PreValue
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// GetTask := ServerTask{
	// 	TaskType: "Get",
	// 	PutAppendargs: nil,
	// 	PutAppendreply: nil,
	// 	Getargs: args,
	// 	Getreply: reply,
	// }
	// kv.AnyChan <- GetTask
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.Dict[args.Key]
	if !ok {
		val = ""
	}
	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// PutTask := ServerTask{
	// 	TaskType: "Put",
	// 	PutAppendargs: args,
	// 	PutAppendreply: reply,
	// 	Getargs: nil,
	// 	Getreply: nil,
	// }
	// kv.AnyChan <- PutTask
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, rpcId := args.ClientId, args.RpcId
	isduplicated, preVal := kv.checkDuplicate(clientId, rpcId)
	// 如果是重复请求，则返回上一次结果
	if isduplicated {
		reply.Value = preVal
		return
	}
	kv.Dict[args.Key] = args.Value

	kv.PrevEntries[clientId] = PrevEntry{
		RpcId: rpcId,
		PreValue: "",
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// AppendTask := ServerTask{
	// 	TaskType: "Append",
	// 	PutAppendargs: args,
	// 	PutAppendreply: reply,
	// 	Getargs: nil,
	// 	Getreply: nil,
	// }
	// kv.AnyChan <- AppendTask

	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, rpcId := args.ClientId, args.RpcId
	isduplicated, preVal := kv.checkDuplicate(clientId, rpcId)
	// 如果是重复请求，则返回上一次结果
	if isduplicated {
		reply.Value = preVal
		return
	}
	key, appendVal := args.Key, args.Value
	currVal, ok := kv.Dict[key]
	if ok {
		kv.Dict[key] = currVal + appendVal
		reply.Value = currVal
	} else {
		kv.Dict[key] = appendVal
		reply.Value = ""
	}

	kv.PrevEntries[clientId] = PrevEntry{
		RpcId: rpcId,
		PreValue: currVal,
	}

}

// func Worker(kv *KVServer) {
// 	for {
// 		select {
// 		case task := <-kv.AnyChan:
// 			// Get operation
// 			if task.TaskType == "Get" {
// 				go func() {
// 					kv.mu.Lock()
// 					defer kv.mu.Unlock()
// 					if value, ok := kv.Dict[task.Getargs.Key]; ok {
// 						task.Getreply.Value = value
// 					} else {
// 						task.Getreply.Value = ""
// 					}
// 				}()
// 			// Put operation
// 			} else if task.TaskType == "Put"{
// 				go func() {
// 					kv.mu.Lock()
// 					defer kv.mu.Unlock()
// 					ClientId, RpcId := task.PutAppendargs.ClientId, task.PutAppendargs.RpcId
// 					isduplicated, prevVal := kv.checkDuplicate(ClientId, RpcId)
// 					if isduplicated {
// 						task.PutAppendreply.Value = prevVal
// 						return
// 					}
// 					kv.Dict[task.PutAppendargs.Key] = task.PutAppendargs.Value
// 					kv.PrevEntries[task.PutAppendargs.ClientId] = PrevEntry{
// 						RpcId: task.PutAppendargs.RpcId,
// 						PreValue: "",
// 					}
// 				}()
// 			// Append operation
// 			} else {
// 				go func() {
// 					kv.mu.Lock()
// 					defer kv.mu.Unlock()
// 					ClientId, RpcId := task.PutAppendargs.ClientId, task.PutAppendargs.RpcId
// 					isduplicated, prevVal := kv.checkDuplicate(ClientId, RpcId)
// 					if isduplicated {
// 						task.PutAppendreply.Value = prevVal
// 						return
// 					}
// 					currVal, ok := kv.Dict[task.PutAppendargs.Key]
// 					if ok {
// 						kv.Dict[task.PutAppendargs.Key] = currVal + task.PutAppendargs.Value
// 						task.PutAppendreply.Value = currVal
// 					} else {
// 						kv.Dict[task.PutAppendargs.Key] = task.PutAppendargs.Value
// 						task.PutAppendreply.Value = ""
// 					}
					
// 					kv.PrevEntries[task.PutAppendargs.ClientId] = PrevEntry{
// 						RpcId: task.PutAppendargs.RpcId,
// 						PreValue: currVal,
// 					}
// 				}()
// 			}
// 		}
// 	}
// }

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Dict = make(map[string]string, 0)
	kv.PrevEntries = make(map[int64]PrevEntry, 0)
	// go Worker(kv)
	return kv
}
