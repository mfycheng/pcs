package main

import (
	"fmt"
	"github.com/mfycheng/pcs"
	"os"
)

func onComplete(collisions, total int) {
	fmt.Println()
	fmt.Printf("Detected %v/%v collisions\n", collisions, total)
}

func main() {
	if len(os.Args) != 1 {
		fmt.Println("Must specify root path")
	}

	path := os.Args[1]
	fmt.Printf("Perfoming Scan on: %v...\n\n", path)

	pcs.ProcessDirectory(path, onComplete)
}
