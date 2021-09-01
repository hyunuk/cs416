// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/DistributedClocks/tracing"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
	Token       tracing.TracingToken
}

type KVS struct {
	notifyCh NotifyChannel
	frontend *rpc.Client
	clientId string
	trace    *tracing.Trace
	opId     uint32
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
		frontend: nil,
		opId:     0,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	d.trace = localTracer.CreateTrace()
	d.clientId = clientId
	log.Printf("dialing frontend at %s", frontEndAddr)
	frontend, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing frontend: %s", err)
	}
	d.trace.RecordAction(KvslibBegin{ClientId: clientId})
	d.frontend = frontend
	d.notifyCh = make(NotifyChannel, chCapacity)
	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	trace := tracer.CreateTrace()
	args := KvslibGet{clientId, d.opId, key, nil}; d.opId++
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	reply := ResultStruct{OpId: args.OpId, StorageFail: false}
	call := d.frontend.Go("FrontEndRPCHandler.Get", args, &reply, nil)

	<-call.Done

	if call.Error != nil {
		log.Fatal(call.Error)
	} else {
		trace = tracer.ReceiveToken(reply.Token)
		trace.RecordAction(KvslibGetResult{reply.OpId, key, reply.Result, reply.StorageFail})
		d.notifyCh <- reply
	}
	return reply.OpId, nil
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	trace := tracer.CreateTrace()
	args := KvslibPut{clientId, d.opId, key, value, nil}; d.opId++
	trace.RecordAction(args)
	args.Token = trace.GenerateToken()
	reply := ResultStruct{OpId: args.OpId, StorageFail: false}
	call := d.frontend.Go("FrontEndRPCHandler.Put", args, &reply, nil)

	<-call.Done

	if call.Error != nil {
		log.Fatal(call.Error)
	} else {
		trace = tracer.ReceiveToken(reply.Token)
		trace.RecordAction(KvslibPutResult{reply.OpId, reply.StorageFail})
		d.notifyCh <- reply
	}
	return reply.OpId, nil
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	err := d.frontend.Close()
	if err != nil {
		return err
	}
	d.trace.RecordAction(KvslibComplete{d.clientId})
	d.frontend = nil
	log.Println("Closed Frontend connection")
	return nil
}
