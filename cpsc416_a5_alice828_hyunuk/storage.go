package distkvs

import (
	"encoding/json"
	"example.org/cpsc416/a5/kvslib"
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

type StorageLoadSuccess struct {
	State map[string]string
}

type StoragePut struct {
	Key   string
	Value string
	Token tracing.TracingToken
}

type StorageSaveData struct {
	Key   string
	Value string
}

type StorageGet struct {
	Key   string
	Token tracing.TracingToken
}

type StorageGetResult struct {
	Key   string
	Value *string
}

type Storage struct {
	// state may go here
}

type StorageRPCHandler struct {
	// key-val store
	store map[string]string
	front *rpc.Client
	trace *tracing.Tracer
	tasks Tasks
	fpath string
}

type Tasks struct {
	mu    sync.Mutex
	tasks map[string][]Task
}

type Task struct {
	value string
	done  chan bool
}

func (*Storage) Start(frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	store := make(map[string]string)
	trace := strace.CreateTrace()

	// TODO: revisit cached key-val store logic
	path, _ := os.Getwd()
	fpath := filepath.Join(path, diskPath, "store.json")
	if exist, err := exists(fpath); err != nil {
		log.Fatal(err)
	} else {
		if exist {
			data, err := ioutil.ReadFile(fpath)
			if err != nil {
				log.Fatal(err)
			} else {
				_ = json.Unmarshal(data, &store)
			}
		} else {
			log.Println("No key-value store to be recovered")
		}
		trace.RecordAction(StorageLoadSuccess{store})
	}

	front, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return err
	}

	server := rpc.NewServer()
	err = server.Register(
		&StorageRPCHandler{store, front, strace, Tasks{tasks: make(map[string][]Task)}, fpath},
	) // publish storage<->frontend procs
	if err != nil {
		return err
	}

	client, err := net.Listen("tcp", storageAddr)
	if err != nil {
		return err
	}

	go server.Accept(client)
	err = front.Call("FrontEndRPCHandler.Start", storageAddr, nil)
	log.Println("storage is online")

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
	trace.RecordAction(StorageGetResult{args.Key, reply.Result})
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
	// https://www.golangprograms.com/golang-writing-struct-to-json-file.html
	f, _ := json.MarshalIndent(s.store, "", "")
	err := ioutil.WriteFile(s.fpath, f, 0644)
	if err != nil {
		return err
	}
	trace.RecordAction(StorageSaveData{args.Key, args.Value})
	reply.Token = trace.GenerateToken()
	done <- true
	return nil
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
