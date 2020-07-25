package dispatcher

import (
	"log"
	"os"
	"sortmassive/internal/pkg/env"
	"sortmassive/internal/pkg/worker"
	"sync"
)

type dispatcher struct {
	start int
	end   int
	done  chan int
	err   chan error
}

var wg sync.WaitGroup
var mrw sync.RWMutex

func Error(err error) {
	if err != nil {
		log.Fatalf("Cannot open file: %s", err)
	}

}

func calculateRemainingBytes(fp *os.File, chunk int64) int64 {
	var bytes int64
	chr := make([]byte, 1)
	fp.Seek(chunk, 0)
	for string(chr[0]) != "\n" {
		fp.ReadAt(chr, bytes)
		bytes++
	}
	return bytes
}

func Dispatch(memory uint64) {
	var chunkSize, start int64
	var workers int

	config := env.GetEnvVars()
	filename := config.File
	fi, err := os.Stat(filename)
	Error(err)
	fileSize := fi.Size()

	fr, err := os.Open(filename)
	defer fr.Close()
	Error(err)

	fw, err := os.Create(filename + "_output.txt")
	defer fw.Close()
	Error(err)

	/*if fileSize <= int64(1*1024*1024) {
		//If file is less than 1MB no need to cut file into chunks
		workers = 1
		chunkSize = fileSize
	} else*/if fileSize < int64(memory) {
		// As of now chunk size will be 20% of filesize
		chunkSize = (20 * fileSize) / 100
		workers = int(fileSize / chunkSize)
	}

	wg.Add(workers)
	var end int64
	for ; workers > 0; workers-- {
		end = start + chunkSize
		addBytes := calculateRemainingBytes(fr, end)
		end += addBytes

		go worker.Run(start, end, fr, fw, &wg, &mrw)

		start += end
	}
	wg.Wait()
}
