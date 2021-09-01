package main

import (
	distpow "example.org/cpsc416/a2"
	"example.org/cpsc416/a2/powlib"
	"github.com/DistributedClocks/tracing"
	"log"
	"math"
	"net"
	"net/rpc"
	"strconv"
)

type Coordinator int

var tracer *tracing.Tracer
var config distpow.CoordinatorConfig
var secrets = make(map[string]*Status)
var resultSent = make(map[string]bool)
var workers = make(map[string][]chan *rpc.Call)
var works []string

type Status struct {
	Found bool
	Result *distpow.WorkerResult
}

func (t *Coordinator) Mine(args *powlib.PowlibMine, reply *[]uint8) error {
	tracer.RecordAction(distpow.CoordinatorMine{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros})
	worksKey := worksJoiner(args.Nonce, args.NumTrailingZeros)
	works = append(works, worksKey)
	threadBits := uint(math.Log2(float64(len(config.Workers))))
	remainderBits := 8 - (threadBits % 9)

	secrets[worksKey] = &Status{false, nil}

	for workerByte, workerAddr := range config.Workers {
		worker, err := rpc.Dial("tcp", string(workerAddr))
		if err != nil {
			return err
		}
		tracer.RecordAction(distpow.CoordinatorWorkerMine{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, WorkerByte: uint8(workerByte)})
		req := distpow.CustomMiner{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, WorkerByte: uint8(workerByte), RemainderBits: uint(remainderBits)}
		workerCall := worker.Go("Worker.Mine", req, nil, nil)
		workers[worksKey] = append(workers[worksKey], workerCall.Done)
	}

	for _, d := range workers[worksKey] {
		<-d
	}

	done := secrets[worksKey].Result
	tracer.RecordAction(distpow.CoordinatorSuccess{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, Secret: done.Secret})
	*reply = done.Secret
	return nil
}

func stringJoiner(args *distpow.WorkerResult) string {
	key := ""
	for _, v := range args.Nonce {
		key += strconv.Itoa(int(v))
	}
	key += strconv.Itoa(int(args.NumTrailingZeros))
	key += strconv.Itoa(int(args.WorkerByte))
	return key
}

func worksJoiner(nonce []uint8, numTrailingZeros uint) string {
	key := ""
	for _, v := range nonce {
		key += strconv.Itoa(int(v))
	}
	key += strconv.Itoa(int(numTrailingZeros))
	return key
}

func (t *Coordinator) Result(args *distpow.WorkerResult, reply *int) error {
	key := stringJoiner(args)
	workerKey := worksJoiner(args.Nonce, args.NumTrailingZeros)
	tracer.RecordAction(distpow.CoordinatorWorkerResult{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, WorkerByte: args.WorkerByte, Secret: args.Secret})
	if status, _ := secrets[workerKey]; status.Found {
		return nil
	}
	secrets[workerKey].Found = true
	secrets[workerKey].Result = args
	for workerByte, workerAddr := range config.Workers {
		temp := &distpow.WorkerResult{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, WorkerByte: uint8(workerByte)}
		key = stringJoiner(temp)
		if resultSent[key] {
			continue
		}
		resultSent[key] = true
		worker, err := rpc.Dial("tcp", string(workerAddr))
		if err != nil {
			return err
		}
		if args.WorkerByte != uint8(workerByte) {
			tracer.RecordAction(distpow.CoordinatorWorkerCancel{Nonce: args.Nonce, NumTrailingZeros: args.NumTrailingZeros, WorkerByte: uint8(workerByte)})
			worker.Go("Worker.Cancel", temp, nil, nil)
		}
	}
	return nil
}

func listen(listenAddr string) {
	client, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	inbound, err := net.ListenTCP("tcp", client)
	if err != nil {
		log.Fatal(err)
	}
	defer inbound.Close()
	rpc.Accept(inbound)
}

func main() {
	err := distpow.ReadJSONConfig("config/coordinator_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	tracer = tracing.NewTracer(tracing.TracerConfig{config.TracerServerAddr, "coordinator", config.TracerSecret})
	rpc.Register(new(Coordinator))
	go listen(config.ClientAPIListenAddr)
	go listen(config.WorkerAPIListenAddr)
	select {}
}
