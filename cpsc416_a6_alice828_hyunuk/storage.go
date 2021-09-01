package distkvs

import (
	"encoding/json"
	"example.org/cpsc416/a6/kvslib"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageState map[string]string

type StorageStartArgs struct {
	StorageId string
	Address   StorageAddr
	Token     tracing.TracingToken
}

type StorageLoadSuccess struct {
	StorageID string
	State     StorageState
}

type StoragePut struct {
	StorageID string
	Key       string
	Value     string
	Token     tracing.TracingToken
}

type StorageSaveData struct {
	StorageID string
	Key       string
	Value     string
}

type StorageGet struct {
	StorageID string
	Key       string
	Token     tracing.TracingToken
}

type StorageGetResult struct {
	StorageID string
	Key       string
	Value     *string
}

type StorageJoining struct {
	StorageID string
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

type Storage struct {
	address   StorageAddr
	connected bool
	storageId string
}

type StorageRPCHandler struct {
	storageId string
	// key-val store
	store map[string]string
	front *rpc.Client
	trace *tracing.Tracer
	tasks Tasks
	path  string
}

type Tasks struct {
	mu    sync.Mutex
	tasks map[string][]Task
}

type Task struct {
	value string
	done  chan bool
}

func (*Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	store := make(map[string]string)
	trace := strace.CreateTrace()

	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, diskPath, "store.json")
	if exist, err := exists(path); err != nil {
		log.Fatal(err)
	} else {
		if exist {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				log.Fatal(err)
			} else {
				_ = json.Unmarshal(data, &store)
			}
		} else {
			log.Println("No key-value store to be recovered")
		}
		trace.RecordAction(StorageLoadSuccess{storageId, store})
	}

	front, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return err
	}

	handler := StorageRPCHandler{storageId, store, front, strace, Tasks{tasks: make(map[string][]Task)}, path}

	server := rpc.NewServer()
	err = server.Register(&handler) // publish storage<->frontend process
	if err != nil {
		return err
	}
	client, err := net.Listen("tcp", storageAddr)
	if err != nil {
		return err
	}

	go server.Accept(client)

	trace.RecordAction(StorageJoining{storageId})

	token := trace.GenerateToken()
	args := StorageStartArgs{storageId, StorageAddr(storageAddr), token}
	var reply map[string]string
	err = front.Call("FrontEndRPCHandler.Start", args, &reply)
	// TODO: should we check opId here?
	//for key, val := range reply {
	//	handler.store[key] = val
	//}

	// TODO: merge reply to store (but not here.. maybe create store as a global var or implement another RPC)
	trace.RecordAction(StorageJoined{storageId, store})

	select {}
}

func (s *StorageRPCHandler) Get(args StorageGet, reply *kvslib.ResultStruct) error {
	trace := s.trace.ReceiveToken(args.Token)
	done := make(chan bool, 1)
	t := s.tasks.tail(args.Key)
	s.tasks.enqueue(args.Key, done)
	if t != nil {
		<-t
	}
	trace.RecordAction(args)
	if val, exist := s.store[args.Key]; exist {
		reply.Result = &val
	}
	trace.RecordAction(StorageGetResult{s.storageId, args.Key, reply.Result})
	reply.Token = trace.GenerateToken()
	done <- true
	return nil
}

func (s *StorageRPCHandler) Put(args StoragePut, reply *kvslib.ResultStruct) error {
	trace := s.trace.ReceiveToken(args.Token)
	done := make(chan bool, 1)
	t := s.tasks.tail(args.Key)
	s.tasks.enqueue(args.Key, done)
	if t != nil {
		<-t
	}
	trace.RecordAction(args)
	s.store[args.Key] = args.Value
	err := save(s.store, s.path)
	if err != nil {
		return err
	}
	trace.RecordAction(StorageSaveData{s.storageId, args.Key, args.Value})
	reply.Token = trace.GenerateToken()
	done <- true
	return nil
}

func (s *StorageRPCHandler) State(_ StorageGet, reply *StorageState) error {
	// TODO: merge it to the reply
	*reply = s.store
	return nil
}

func (s *StorageRPCHandler) Join(args StorageState, _ *string) error {
	// TODO: should we check opId here?
	for key, val := range args {
		s.store[key] = val
	}
	println(s.storageId, "updates are done", args)
	return nil
}

// https://www.golangprograms.com/golang-writing-struct-to-json-file.html
// save saves the current storage state after Put operation.
func save(store StorageState, path string) error {
	f, _ := json.MarshalIndent(store, "", "")
	err := ioutil.WriteFile(path, f, 0644)
	return err
}

// https://stackoverflow.com/a/22467409
// exists returns true if the file exists; false otherwise.
func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (t *Tasks) head(key string) (done chan bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.empty(key) {
		return
	}
	done = t.tasks[key][0].done
	t.tasks[key] = t.tasks[key][1:]
	return
}

func (t *Tasks) tail(key string) (done chan bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.empty(key) {
		return
	}
	n := len(t.tasks[key])
	done = t.tasks[key][n-1].done
	t.tasks[key] = t.tasks[key][:n-1]
	return
}

func (t *Tasks) enqueue(key string, done chan bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	task := Task{key, done}
	if tasks := t.tasks[key]; tasks != nil {
		t.tasks[key] = append(t.tasks[key], task)
	} else {
		t.tasks[key] = []Task{task}
	}
}

func (t *Tasks) empty(key string) bool {
	return len(t.tasks[key]) == 0
}
