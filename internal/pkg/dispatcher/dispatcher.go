package dispatcher

import (
	"io/ioutil"
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

func Dispatch(memory uint64) {
	var chunkSize, start int64
	var workers int

	config := env.GetEnvVars()
	filename := config.File
	fi, err := os.Stat(filename)
	Error(err)
	fileSize := fi.Size()

	if fileSize < int64(1024*1024) {
		workers = 1
		chunkSize = fileSize
		//TODO Handle error here
		contents, _ := ioutil.ReadFile(filename)
		str := string(contents)
	} else if fileSize < int64(memory) {
		chunkSize = (20 * fileSize) / 100
		workers = int(fileSize / chunkSize)
	}

	wg.Add(workers)
	fr, err := os.Open(filename)
	defer fr.Close()
	Error(err)
	fw, err := os.Create("/tmp/output.txt")
	defer fw.Close()
	Error(err)
	for ; workers > 0; workers-- {
		go worker.Run(start, start+chunkSize, fr, fw, &wg, &mrw)
		start = start + chunkSize
	}
	wg.Wait()
}
