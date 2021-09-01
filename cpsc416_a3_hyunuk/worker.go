package distpow

type WorkerConfig struct {
	WorkerID         string
	ListenAddr       string
	CoordAddr        string
	TracerServerAddr string
	TracerSecret     []byte
}

type WorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type WorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type WorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}
