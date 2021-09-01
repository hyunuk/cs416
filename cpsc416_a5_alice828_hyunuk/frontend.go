package distkvs

import (
	"example.org/cpsc416/a5/kvslib"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"github.com/DistributedClocks/tracing"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct{}

type FrontEndStorageFailed struct{}

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

type FrontEnd struct {
	// state may go here
}

type FrontEndRPCHandler struct {
	duration  time.Duration
	ftrace    *tracing.Tracer
	storage   StorageAddr
	connected bool
	started   chan StorageAddr
	failed    chan bool
}

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	trace := ftrace.CreateTrace()
	handler := FrontEndRPCHandler{
		duration: time.Duration(storageTimeout) * time.Second,
		ftrace:   ftrace,
		started:  make(chan StorageAddr),
		failed:   make(chan bool),
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
		case handler.storage = <-handler.started:
			handler.connected = true
			trace.RecordAction(FrontEndStorageStarted{})
		case <-handler.failed:
			handler.connected = false
			trace.RecordAction(FrontEndStorageFailed{})
		}
	}
	// storage has been started
}

func (f *FrontEndRPCHandler) Start(args StorageAddr, reply *string) error {
	f.started <- args
	return nil
}

func (f *FrontEndRPCHandler) Get(args FrontEndGet, reply *kvslib.ResultStruct) error {
	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	storage, err := rpc.Dial("tcp", string(f.storage))
	if err != nil {
		if f.connected {
			f.failed <- true
		}
	} else {
		for i := 0; i < 2; i++ {
			call := storage.Go("StorageRPCHandler.Get", args, &reply, nil)
			if success := try(call, f.duration); success {
				trace = f.ftrace.ReceiveToken(reply.Token)
				trace.RecordAction(FrontEndGetResult{args.Key, reply.Result, false})
				reply.Token = trace.GenerateToken()
				return nil
			}
		}
	}
	// re-tried and failed
	trace.RecordAction(FrontEndGetResult{args.Key, reply.Result, true})
	reply.StorageFail = true
	reply.Token = trace.GenerateToken()
	return nil
}

func (f *FrontEndRPCHandler) Put(args FrontEndPut, reply *kvslib.ResultStruct) error {
	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	storage, err := rpc.Dial("tcp", string(f.storage))
	if err != nil {
		if f.connected {
			f.failed <- true
		}
	} else {
		for i := 0; i < 2; i++ {
			call := storage.Go("StorageRPCHandler.Put", args, &reply, nil)
			if success := try(call, f.duration); success {
				trace = f.ftrace.ReceiveToken(reply.Token)
				trace.RecordAction(FrontEndPutResult{false})
				reply.Token = trace.GenerateToken()
				return nil
			}
		}
	}
	// re-tried and failed
	trace.RecordAction(FrontEndPutResult{true})
	reply.StorageFail = true
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
