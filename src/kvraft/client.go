package kvraft

import (
	"../labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId  int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	atomic.AddInt64(&ck.requestId, 1)		//这里我们保证请求编号自增的原子性
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	reply := GetReply{}
	for {
		//time.Sleep(10 * time.Millisecond)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			value = reply.Value
		case ErrNoKey, ErrRRequest:
		case ErrWrongLeader, ErrNotOp:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	atomic.AddInt64(&ck.requestId, 1)
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	reply := PutAppendReply{}

	for {
		//time.Sleep(10 * time.Millisecond)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
		case ErrRRequest:

		case ErrWrongLeader, ErrNotOp:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
