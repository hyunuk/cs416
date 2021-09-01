package hash_miner

import (
	"crypto/md5"
	"github.com/DistributedClocks/tracing"
	"sync"
)

type WorkerStart struct {
	ThreadByte uint8
}

type WorkerSuccess struct {
	ThreadByte uint8
	Secret     []uint8
}

type WorkerCancelled struct {
	ThreadByte uint8
}

type MiningBegin struct{}

type MiningComplete struct {
	Secret []uint8
}

func Mine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeroes, threadBits uint) ([]uint8, error) {
	tracer.RecordAction(MiningBegin{})
	numThreads := 1 << threadBits
	found := false
	secret := make(chan []uint8, numThreads)
	wait := new(sync.WaitGroup)
	wait.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func(id uint8) {
			defer wait.Done()
			tracer.RecordAction(WorkerStart{id})
			suffix := []uint8{id}
			for {
				if found {
					tracer.RecordAction(WorkerCancelled{id})
					return
				}

				hash := md5.Sum(append(nonce, suffix...))

				if checkZeroes(hash, numTrailingZeroes) {
					tracer.RecordAction(WorkerSuccess{id, suffix})
					found = true
					secret <- suffix
					return
				}

				val := uint16(0)
				carry := 1 << threadBits
				for i := 0; i < len(suffix) && carry > 0; i++ {
					val = uint16(suffix[i]) + uint16(carry)
					suffix[i] = uint8(val)
					carry = int(val >> 8)
				}
				if carry > 0 {
					val = uint16(carry)
					suffix = append(suffix, uint8(carry))
				}
			}
		}(uint8(i))
	}
	wait.Wait()
	result := <-secret
	tracer.RecordAction(MiningComplete{result})
	return result, nil
}

func checkZeroes(hash [16]byte, numTrailingZeroes uint) bool {
	isOdd := numTrailingZeroes%2 != 0
	l := (len(hash) - 1) - (int(numTrailingZeroes) / 2)
	if isOdd && (hash[l]&0xf) != 0 {
		return false
	}

	for r := len(hash) - 1; r > l; r-- {
		if hash[r] != 0 {
			return false
		}
	}
	return true
}
