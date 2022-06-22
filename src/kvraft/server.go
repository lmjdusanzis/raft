package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key  string
	Value string
	Operation string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// Your definitions here.

	preTime    time.Time
	Timer      time.Duration

	kvDB   map[string]string

	clientMap         		map[int64]int64    				//服务器上已经处理的Client最新的的RequetId
	commandIndexMap        	map[int]raft.ApplyMsg			//数据库正要执行的命令
	lastExecutedIndex 		int								//数据库已经执行的最后一个命令的索引

	waitApplyCond     *sync.Cond
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Key: args.Key,
		Operation: "Get",
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	commandIndex, term, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("GET from client %v request %v Index-%v ",
		args.ClientId, args.RequestId, commandIndex)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.commandIndexMap[commandIndex] = raft.ApplyMsg{}				//我们向需要执行的索引项里加入的空的raft.ApplyMsg{}，
	defer delete(kv.commandIndexMap, commandIndex)					//判断该commandIndex处的命令是否被执行，被第几任期的
																	//Leader执行
	preTime := time.Now()
	awakenAll(&kv.mu, kv.waitApplyCond, kv.Timer)					//防止无限等待

	for kv.lastExecutedIndex < commandIndex && time.Now().Sub(preTime) < kv.Timer {
		kv.waitApplyCond.Wait()							//如果server还没有执行到该条命令，并且还没超时，我们继续等待
	}

	if time.Now().Sub(preTime) >= kv.Timer {			//超时则还没执行，我们通知客户端执行失败
		reply.Err = ErrNotOp
		DPrintf("GET timeout to client %v request %v Index-%v",
			args.ClientId, args.RequestId, commandIndex)
		return
	}

	executedMsg := kv.commandIndexMap[commandIndex]
	if executedMsg.CommandTerm != term {
		DPrintf("GET ErrorTerm to client %v request %v Index-%v",
			args.ClientId, args.RequestId, commandIndex)			  //如果把命令传给raft时的任期和实际提交的任期不一致
		reply.Err = ErrNotOp							             //我们认为该条Get命令执行失败，实际上执行成功了。这里主要是为了
		return											  //保证一致性，不要读到过去执行的结果。让client重发
	}

	value, ok := kv.kvDB[args.Key]						//执行成功，给出返回值
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return
}

func awakenAll(mu *sync.Mutex, cond *sync.Cond, duration time.Duration)  {
	go func() {
		time.Sleep(duration)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	currentRequestId, ok := kv.clientMap[args.ClientId]           			 // 判断收到的是不是过期和重复的报文
	if ok && currentRequestId >= args.RequestId {
		reply.Err = ErrRRequest
		DPrintf("PUT Repeated to client %v request %v",
			args.ClientId, args.RequestId)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		Key: args.Key,
		Value: args.Value,
		Operation: args.Op,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	commandIndex, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("PUT from client %v request %v Index-%v",
		args.ClientId, args.RequestId, commandIndex)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.commandIndexMap[commandIndex] = raft.ApplyMsg{}
	defer delete(kv.commandIndexMap, commandIndex)

	preTime := time.Now()
	awakenAll(&kv.mu, kv.waitApplyCond, kv.Timer)

	for kv.lastExecutedIndex < commandIndex && time.Now().Sub(preTime) < kv.Timer {
		kv.waitApplyCond.Wait()
	}

	if time.Now().Sub(preTime) >= kv.Timer {
		reply.Err = ErrNotOp
		DPrintf("PUT timeout to client %v request %v Index-%v",
			args.ClientId, args.RequestId, commandIndex)
		return
	}

	executedMsg := kv.commandIndexMap[commandIndex]
	if executedMsg.CommandTerm != term {
		reply.Err = ErrNotOp
		DPrintf("PUT ErrorTerm to client %v request %v Index-%v",
			args.ClientId, args.RequestId, commandIndex)
		return
	}

	reply.Err = OK
	return
}


func (kv *KVServer) runkvDB() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {							//无效的命令只有一个，就是建立快照的命令
			DPrintf("Do snapshot command")
			rawSnapshot := msg.Command.([]byte)			//从raft层传来的被序列化的snapshot，需要被反序列化应用到kvDB
			kv.deserializeKVState(rawSnapshot)
			continue
		}

		command := msg.Command.(Op)						//对于其他命令，我们读取命令的内容
		kv.mu.Lock()

		if command.RequestId <= kv.clientMap[command.ClientId]  {			//如果该命令比该客户端在本服务器上执行过的
			DPrintf("Not Execute client %v request %v Index %v, Request Repeated",		//最新要旧，我们拒绝它。
				command.ClientId, command.RequestId, msg.CommandIndex)		//这是一个过期或者重复的命令，我们不必执行
		} else {
			switch command.Operation {
			case "Get":
				DPrintf("Execute GET client %v request %v Index %v",
					command.ClientId, command.RequestId, msg.CommandIndex)
			case "Put":
				kv.kvDB[command.Key] = command.Value
				DPrintf("Execute APPEND client %v request %v Index %v",
					command.ClientId, command.RequestId, msg.CommandIndex)
			case "Append":
				kv.kvDB[command.Key] = kv.kvDB[command.Key] + command.Value
				DPrintf("Execute PUT client %v request %v Index %v",
					command.ClientId, command.RequestId, msg.CommandIndex)
			default:
				DPrintf("Error Request")
			}
			kv.clientMap[command.ClientId] = command.RequestId			//执行该最新的命令，并client在server上已经执行的
		}

		kv.lastExecutedIndex = msg.CommandIndex							//同时，记录该日志为服务器上已执行的最新日志索引
		kv.waitApplyCond.Broadcast()									//执行完成后，唤醒等待结果reply
		if _, ok := kv.commandIndexMap[msg.CommandIndex]; ok {			//我们把该日志项标记成已执行
			kv.commandIndexMap[msg.CommandIndex] = msg
		}

		kv.checkRaftState(msg.CommandIndex, msg.CommandTerm)     //执行了这么长时间，检查一下是不是要raft层做一下快照

		if kv.killed() {
			kv.mu.Unlock()
			break
		}

		kv.mu.Unlock()
	}
}

//我们要做快照，应该把快照发给raft层，告诉raft快照是做到了哪个日志索引，以及对应的任期
func (kv *KVServer) checkRaftState(index int, term int)  {
	if kv.maxraftstate <= 0 ||
		kv.persister.RaftStateSize() < int(float64((kv.maxraftstate) * 2.0 / 3)) {
		return
	}
	DPrintf("Close to maxraftstate, Do snapshot please")
	rawSnapshot := kv.serializeKVState()                           	//不要把序列化kvstate的过程放在协程里
	go func() {															//否则会导致读写竞争问题！！！
		kv.rf.TakeSnapshot(index, term, rawSnapshot)
	}()
}

func (kv *KVServer) serializeKVState() []byte  {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.clientMap)

	data := w.Bytes()
	return data
}

func (kv *KVServer) deserializeKVState(data []byte)  {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[string]string
	var clientMap map[int64]int64
	if d.Decode(&kvDB) != nil || d.Decode(&clientMap) != nil {
		DPrintf("fail to decode KVState")
	} else {
		kv.mu.Lock()
		kv.kvDB = kvDB
		kv.clientMap = clientMap
		DPrintf("successed to decode KVState")
		kv.mu.Unlock()
	}
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.preTime = time.Now()
	kv.Timer = 500 * time.Millisecond
	kv.kvDB = make(map[string]string)
	kv.clientMap = make(map[int64]int64)
	kv.waitApplyCond = sync.NewCond(&kv.mu)
	kv.commandIndexMap = make(map[int]raft.ApplyMsg)

	kv.deserializeKVState(kv.persister.ReadSnapshot())

	go kv.runkvDB()

	return kv
}
