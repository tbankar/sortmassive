package dispatcher

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sortmassive/internal/pkg/env"
	"sortmassive/internal/pkg/worker"
	"strconv"
	"sync"
)

type MergeData struct {
	StartByte int64
	EndByte   int64
	Number    int64
}

var wg sync.WaitGroup
var mrw sync.RWMutex
var startBytes []int64

func Error(err error) {
	if err != nil {
		log.Fatalf("Cannot open file: %s", err)
	}

}

func calculateRemainingBytes(fp *os.File, chunkSize int64) int64 {
	chr := make([]byte, 1)
	for string(chr[0]) != "\n" {
		_, err := fp.ReadAt(chr, chunkSize)
		if err == io.EOF {
			chunkSize--
			for {
				fp.ReadAt(chr, chunkSize)
				if string(chr[0]) == "\n" {
					return chunkSize
				}
				chunkSize--
			}
		}
		chunkSize++
	}
	return chunkSize
}

func populateStructArray(arrBytes []int64, fp *os.File) []MergeData {
	var res MergeData
	var result []MergeData
	scanner := bufio.NewScanner(fp)
	for i := 0; i < len(arrBytes); i = i + 2 {
		res.StartByte = arrBytes[i]
		res.EndByte = arrBytes[i+1]
		fp.Seek(res.StartByte, 0)
		scanner.Scan()
		str := string(scanner.Text())
		number, err := strconv.Atoi(str)
		if err != nil {
			//TODO add error
		}
		res.Number = int64(number)
		result = append(result, res)
	}
	return result
}

func Dispatch(memory uint64) {
	var chunkSize, startByte int64
	var workers int

	config := env.GetEnvVars()
	filename := config.File
	fi, err := os.Stat(filename)
	Error(err)
	fileSize := fi.Size()

	fr, err := os.Open(filename)
	defer fr.Close()
	Error(err)

	fw, err := os.OpenFile(filename+"_output.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
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
	var endByte int64
	var lastChunk bool
	for ; workers > 0; workers-- {
		endByte = startByte + chunkSize
		addBytes := calculateRemainingBytes(fr, endByte)
		if endByte > addBytes {
			lastChunk = true
		}
		endByte = addBytes

		worker.Run(startByte, endByte, fr, fw, &wg, &mrw)
		if lastChunk {
			return
		}

		startByte = endByte
		startBytes = append(startBytes, startByte, endByte)
	}
	wg.Wait()

	arrMerge := populateStructArray(startBytes, fr)
	var num int
	for len(arrMerge) != 0 {
		len1 := len(arrMerge)
		for i := 0; i < len1; i++ {
			if arrMerge[i].Number < arrMerge[i+1].Number && i+1 < len1 {
				num = i
			}
		}
		fmt.Println(num)

		//arrMerge[num].Number =
	}
}
