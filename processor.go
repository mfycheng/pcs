package pcs

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

const (
	PIECE_MAPPERS         = 8
	HASH_BUFFER_POOL_SIZE = 20
)

var (
	MapperWG  sync.WaitGroup
	ReducerWG sync.WaitGroup

	hashBuffer = make(chan []byte, HASH_BUFFER_POOL_SIZE)
	results    = make(chan string)
)

type OnComplete func(collisions, total int)

func init() {
	for i := 0; i < HASH_BUFFER_POOL_SIZE; i++ {
		hashBuffer <- make([]byte, sha256.Size)
	}
}

func pieceMapper(input chan []byte, pool chan []byte) {
	defer MapperWG.Done()

	for p := range input {
		// Grab a hash buffer
		hb := <-hashBuffer

		hash := sha256.New()
		hash.Write(p)
		hashString := hex.EncodeToString(hash.Sum(nil))

		pool <- p
		hashBuffer <- hb
		results <- hashString
	}

}

func pieceReducer(onComplete OnComplete) {
	defer ReducerWG.Done()

	hashes := make(map[string]bool)
	collisions := 0

	for h := range results {
		if _, exists := hashes[h]; exists {
			collisions++
		} else {
			hashes[h] = true
		}
	}

	onComplete(collisions, len(hashes)+collisions)
}

func LaunchProcessors(input chan []byte, pool chan []byte, onComplete OnComplete) {
	// Launch mappers
	MapperWG.Add(PIECE_MAPPERS)
	for i := 0; i < PIECE_MAPPERS; i++ {
		go pieceMapper(input, pool)
	}

	// Launch reducers
	ReducerWG.Add(1)
	go pieceReducer(onComplete)
}

func WaitForProcessors() {
	// We know the input channels are closed, so we're
	// just waiting for the last of them to flush
	MapperWG.Wait()

	// Mappers are done, so wait for the reducer to finish.
	close(results)
	ReducerWG.Wait()

	// Final cleanup (not really needed)
	close(hashBuffer)
}
