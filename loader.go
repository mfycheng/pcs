package pcs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	MAX_FILE_QUEUE         = 200
	FILE_LOADERS           = 200
	PIECE_BUFFER_POOL_SIZE = 200
	PIECE_SIZE             = 2 << 21
)

var (
	LoaderWG sync.WaitGroup

	fileQueue   = make(chan string, MAX_FILE_QUEUE)
	pieceBuffer = make(chan []byte, PIECE_BUFFER_POOL_SIZE)
	pieceQueue  = make(chan []byte)
)

func init() {
	for i := 0; i < PIECE_BUFFER_POOL_SIZE; i++ {
		pieceBuffer <- make([]byte, PIECE_SIZE)
	}
}

func loadPiece() {
	defer LoaderWG.Done()

	for filePath := range fileQueue {
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Could not load file %v: %v. Skipping...\n", filePath, err)
			continue
		}

		for {
			pb := <-pieceBuffer

			n, err := io.ReadFull(file, pb)
			if err == io.EOF {
				pieceBuffer <- pb
				break
			} else if err != nil && (err != io.ErrUnexpectedEOF || n == 0) {
				fmt.Printf("Unexpected error loading file %v: %v\n", filePath, err)
				os.Exit(1)
			}

			pieceQueue <- pb
			_, err = file.Seek(PIECE_SIZE, 1)
			if err != nil {
				fmt.Printf("Unexpected error seeking: %v\n", err)
				os.Exit(1)
			}
		}

		file.Close()
	}

}

func ProcessDirectory(root string, onComplete OnComplete) error {
	// Launch processors
	LaunchProcessors(pieceQueue, pieceBuffer, onComplete)

	// Launch file loaders
	LoaderWG.Add(FILE_LOADERS)
	for i := 0; i < FILE_LOADERS; i++ {
		go loadPiece()
	}

	// Start the pipeline by walking the tree and sending
	// filenames off to the first stage of the pipeline.
	count := 1
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		count++
		fmt.Printf("\rScanning: %v", strconv.Itoa(count))
		fileQueue <- path
		return nil
	})

	fmt.Print("\rScanning: Complete!\n")
	fmt.Println("Waiting for processing...")

	if err != nil {
		return err
	}

	// Wait for the loaders to complete.
	close(fileQueue)
	LoaderWG.Wait()

	// Wait for the mappers to complete
	close(pieceQueue)
	WaitForProcessors()

	// Final cleanup (not really needed)
	close(pieceBuffer)

	return nil
}
