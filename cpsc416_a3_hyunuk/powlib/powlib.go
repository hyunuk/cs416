// Package powlib provides an API which is a wrapper around RPC calls to the
// coordinator.
package powlib

import (
	"net/rpc"

	"github.com/DistributedClocks/tracing"
)

type PowlibMiningBegin struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type PowlibSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type PowlibMiningComplete struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

// MineResult contains the result of a mining request.
type MineResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan MineResult

// POW struct represents an instance of the powlib.
type POW struct {
	Client *rpc.Client
	Chan   NotifyChannel
}

func NewPOW() *POW {
	return &POW{
		Client: nil,
		Chan:   nil,
	}
}

// Initialize Initializes the instance of POW to use for connecting to the coordinator,
// and the coordinators IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by powlib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *POW) Initialize(coordAddr string, chCapacity uint) (NotifyChannel, error) {
	client, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return nil, err
	}
	d.Client = client
	d.Chan = make(NotifyChannel, chCapacity)
	return d.Chan, nil
}

// Mine is a non-blocking request from the client to the system solve a proof
// of work puzzle. The arguments have identical meaning as in A2. In case
// there is an underlying issue (for example, the coordinator cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil.
// Note that this call is non-blocking, and the solution to the proof of work
// puzzle must be delivered asynchronously to the client via the notify-channel
// channel returned in the Initialize call.
func (d *POW) Mine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeros uint) error {
	tracer.RecordAction(PowlibMiningBegin{nonce, numTrailingZeros})
	args := &PowlibMine{nonce, numTrailingZeros}
	tracer.RecordAction(*args)
	var reply []uint8
	err := d.Client.Call("Coordinator.Mine", args, &reply)
	if err != nil {
		return err
	}
	tracer.RecordAction(PowlibSuccess{nonce, numTrailingZeros, reply})
	tracer.RecordAction(PowlibMiningComplete{nonce, numTrailingZeros, reply})
	d.Chan <- MineResult{nonce, numTrailingZeros, reply}
	return nil
}

// Close Stops the POW instance from communicating with the coordinator and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *POW) Close() error {
	err := d.Client.Close()
	if err != nil {
		return err
	}
	return nil
}
