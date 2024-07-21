package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type KVTask struct {
	TaskType string
	Key string
	Value string
}

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	// 用于传递任务
	AnyChan chan interface{}
	ClientId int64
	RpcId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.RpcId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	arg := GetArgs{
		Key: key,
	}
	reply := GetReply {}
	for {
		ok := ck.server.Call("KVServer.Get", &arg, &reply)
		if ok {
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	ck.RpcId += 1
	// if op == "Put" {
	// 	putArg := PutAppendArgs {
	// 		Key: key,
	// 		Value: value,
	// 		ClientId: ck.ClientId,
	// 		RpcId: ck.RpcId,
	// 	}
	// 	var replyStr string
	// 	putReply := PutAppendReply {
	// 		Value: replyStr,
	// 	}
	// 	for {
	// 		ok :=ck.server.Call("KVServer."+op, &putArg, &putReply)
	// 		if ok {
	// 			return putReply.Value
	// 		}
	// 	}
	// } else {
	// 	appendArg := PutAppendArgs {
	// 		Key: key,
	// 		Value: value,
	// 		ClientId: ck.ClientId,
	// 		RpcId: ck.RpcId,
	// 	}
	// 	var replyStr string
	// 	appendReply := PutAppendReply {
	// 		Value: replyStr,
	// 	}
	// 	for {
	// 		ok := ck.server.Call("KVServer."+op, &appendArg, &appendReply)
	// 		if ok {
	// 			return appendArg.Value
	// 		}
	// 	}
	// }

	args := PutAppendArgs{
		Key: key,
		Value: value,
		ClientId: ck.ClientId,
		RpcId: ck.RpcId,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
