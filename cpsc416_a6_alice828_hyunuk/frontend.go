package distkvs

import (
	"example.org/cpsc416/a6/kvslib"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"github.com/DistributedClocks/tracing"
)

type StorageAddr string

// FrontEndConfig matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

type FrontEndPut struct {
	Key   string
	Value string
	Token tracing.TracingToken
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key   string
	Token tracing.TracingToken
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type FrontEndStorageJoined struct {
	StorageIds []string
}

type FrontEnd struct {
	// state may go here
}

type StorageStart struct {
	address StorageAddr
	store   StorageState
	id      string
	joined  []string
}

type FrontEndRPCHandler struct {
	duration time.Duration
	ftrace   *tracing.Tracer
	storage  map[string]Storage
	started  chan StorageStart
	failed   chan StorageStart
}

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	// TODO: do we need to record any actions here?
	trace := ftrace.CreateTrace()
	handler := FrontEndRPCHandler{
		duration: time.Duration(storageTimeout) * time.Second,
		ftrace:   ftrace,
		storage:  make(map[string]Storage),
		started:  make(chan StorageStart),
		failed:   make(chan StorageStart),
	}
	server := rpc.NewServer()
	err := server.Register(&handler)
	if err != nil {
		return fmt.Errorf("format of FrontEnd RPCs aren't correct: %s", err)
	}

	clientListener, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", clientAPIListenAddr, err)
	}

	storageListener, err := net.Listen("tcp", storageAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, err)
	}

	go server.Accept(clientListener)
	go server.Accept(storageListener)
	for {
		select {
		case s := <-handler.started:
			//handler.connected = true
			storage, _ := rpc.Dial("tcp", string(s.address))
			_ = storage.Call("StorageRPCHandler.Join", StorageState{}, nil)
			trace.RecordAction(FrontEndStorageStarted{s.id})
			trace.RecordAction(FrontEndStorageJoined{s.joined})
		case s := <-handler.failed:
			//handler.connected = false
			trace.RecordAction(FrontEndStorageFailed{s.id})
			trace.RecordAction(FrontEndStorageJoined{s.joined})
		}
	}
	// storage has been started
}

func (f *FrontEndRPCHandler) Start(args StorageStartArgs, reply *map[string]string) error {
	//trace := f.ftrace.ReceiveToken(args.Token)
	for _, s := range f.storage {
		storage, err := rpc.Dial("tcp", string(s.address))
		if err != nil {
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
			continue
		}
		success := false
		for i := 0; i < 2; i++ {
			call := storage.Go("StorageRPCHandler.State", args, &reply, nil)
			if success = try(call, f.duration); success {
				break
			}
		}
		if !success {
			// TODO: is this necessary?
			//f.storage
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
		}
	}
	s := Storage{address: args.Address, storageId: args.StorageId}
	f.storage[s.storageId] = s
	//f.storage = append(f.storage, s)
	var joined []string
	for id := range f.storage {
		joined = append(joined, id)
	}
	f.started <- StorageStart{args.Address, *reply, s.storageId, joined}
	return nil
}

func (f *FrontEndRPCHandler) Get(args FrontEndGet, reply *kvslib.ResultStruct) error {
	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	ok := 0
	for _, s := range f.storage {
		// TODO: should this go into for loop?
		storage, err := rpc.Dial("tcp", string(s.address))
		if err != nil {
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
			continue
		}
		success := false
		for i := 0; i < 2; i++ {
			call := storage.Go("StorageRPCHandler.Get", args, &reply, nil)
			if success = try(call, f.duration); success {
				ok++
				break
			}
		}
		if !success {
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
		}
	}
	if ok > 0 {
		trace = f.ftrace.ReceiveToken(reply.Token)
	} else {
		reply.StorageFail = true // no storage available at this time
	}
	trace.RecordAction(FrontEndGetResult{args.Key, reply.Result, reply.StorageFail})
	reply.Token = trace.GenerateToken()
	return nil
}

func (f *FrontEndRPCHandler) Put(args FrontEndPut, reply *kvslib.ResultStruct) error {
	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	ok := 0
	for _, s := range f.storage {
		storage, err := rpc.Dial("tcp", string(s.address))
		if err != nil {
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
			continue
		}
		success := false
		for i := 0; i < 2; i++ {
			call := storage.Go("StorageRPCHandler.Put", args, &reply, nil)
			if success = try(call, f.duration); success {
				ok++
				break
			}
		}
		if !success {
			delete(f.storage, s.storageId)
			var joined []string
			for id := range f.storage {
				joined = append(joined, id)
			}
			f.failed <- StorageStart{id: s.storageId, joined: joined}
		}
	}
	if ok > 0 {
		trace = f.ftrace.ReceiveToken(reply.Token)
	} else {
		reply.StorageFail = true // no storage available at this time
	}
	trace.RecordAction(FrontEndPutResult{reply.StorageFail})
	reply.Token = trace.GenerateToken()
	return nil
}

func try(call *rpc.Call, duration time.Duration) bool {
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			}
			return true
		case <-time.After(duration):
			// time out
			return false
		}
	}
}
