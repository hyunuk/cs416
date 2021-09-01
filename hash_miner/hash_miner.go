package hash_miner

import (
	"bytes"
	"crypto/md5"
	"fmt"

	"github.com/DistributedClocks/tracing"
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

func nextChunk(chunk []uint8) []uint8 {
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == 0xFF {
			chunk[i] = 0
		} else {
			chunk[i]++
			return chunk
		}
	}
	return append(chunk, 1)
}

func hasNumZeroesSuffix(str []byte, numZeroes uint) bool {
	var trailingZeroesFound uint
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '0' {
			trailingZeroesFound++
		} else {
			break
		}
	}
	return trailingZeroesFound >= numZeroes
}

func miner(tracer *tracing.Tracer, nonce []uint8, threadByte uint8, trailingZeroesSearch, remainderBits uint, killChan <-chan struct{}, resultChan chan<- []uint8) {
	chunk := []uint8{}
	startingThreadByte := threadByte

	hashStrBuf, wholeBuffer := new(bytes.Buffer), new(bytes.Buffer)
	if _, err := wholeBuffer.Write(nonce); err != nil {
		panic(err)
	}
	wholeBufferTrunc := wholeBuffer.Len()

	// table out all possible "thread bytes", aka the byte prefix
	// between the nonce and the bytes explored by this worker
	remainderEnd := 1 << remainderBits
	threadBytes := make([]uint8, remainderEnd)
	for i := 0; i < remainderEnd; i++ {
		threadBytes[i] = uint8((int(threadByte) << remainderBits) | i)
	}
	tracer.RecordAction(WorkerStart{startingThreadByte})

	for {
		for _, threadByte := range threadBytes {
			// optional: end early
			select {
			case <-killChan:
				tracer.RecordAction(WorkerCancelled{startingThreadByte})
				//fmt.Printf("t = %d cancelled\n", threadId)
				resultChan <- nil
				return
			default:
				// pass
			}
			wholeBuffer.Truncate(wholeBufferTrunc)
			if err := wholeBuffer.WriteByte(threadByte); err != nil {
				panic(err)
			}
			if _, err := wholeBuffer.Write(chunk); err != nil {
				panic(err)
			}
			hash := md5.Sum(wholeBuffer.Bytes())
			hashStrBuf.Reset()
			fmt.Fprintf(hashStrBuf, "%x", hash)
			if hasNumZeroesSuffix(hashStrBuf.Bytes(), trailingZeroesSearch) {
				tracer.RecordAction(WorkerSuccess{
					ThreadByte: startingThreadByte,
					Secret:     wholeBuffer.Bytes()[wholeBufferTrunc:],
				})
				//fmt.Printf("t %d found %v\n", threadId, hashStrBuf)
				resultChan <- wholeBuffer.Bytes()[wholeBufferTrunc:]
				return
			}
		}
		chunk = nextChunk(chunk)
	}
}

func Mine(tracer *tracing.Tracer, nonce []uint8, numTrailingZeroes, threadBits uint) (secret []uint8, err error) {
	threadCount := 1 << threadBits
	remainderBits := 8 - (threadBits % 8)
	tracer.RecordAction(MiningBegin{})
	//fmt.Printf("tc = %d; tb = %d; rb = %d\n", threadCount, threadBits, remainderBits)

	resultChan := make(chan []uint8, threadCount)
	killChan := make(chan struct{}, threadCount)

	for i := 0; i < threadCount; i++ {
		go miner(tracer, nonce, uint8(i), numTrailingZeroes, remainderBits, killChan, resultChan)
	}

	result := <-resultChan
	for i := 0; i < threadCount; i++ {
		killChan <- struct{}{}
	}
	for i := 0; i < threadCount-1; i++ {
		<-resultChan
	}
	close(resultChan)
	tracer.RecordAction(MiningComplete{result})

	return result, nil
}
