package main

import (
	"crypto/md5"
	"log"

	"example.org/cpsc416/a2/hash_miner"
	"github.com/DistributedClocks/tracing"
)

func main() {
	tracingServer := tracing.NewTracingServerFromFile("tracing_server_config.json")
	err := tracingServer.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer tracingServer.Close()
	go tracingServer.Accept()

	tracer := tracing.NewTracerFromFile("tracing_config.json")
	defer tracer.Close()
	result, err := hash_miner.Mine(tracer, []uint8{1, 2, 3, 4}, 7, 4)
	log.Printf("First result returned: %v, %v\n", result, err)

	// how to check the mining result (notice the %x format!)
	concat := []uint8{1, 2, 3, 4}
	concat = append(concat, result...)
	checksum := md5.Sum(concat)
	log.Printf("hashes to %x, which should end in 7 zeroes\n", checksum)

	// result, err = hash_miner.Mine(tracer, []uint8{1, 2, 3, 4}, 7, 4)
	// log.Printf("Repeat result returned: %v, %v\n", result, err)
}
