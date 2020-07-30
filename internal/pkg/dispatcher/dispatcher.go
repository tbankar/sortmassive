package dispatcher

import (
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
	Len       int64
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

func readChars(fp *os.File, offset int64) ([]byte, int64) {
	var bytes []byte
	chr := make([]byte, 1)
	for {
		fp.ReadAt(chr, offset)
		if chr[0] == '\n' {
			break
		}
		bytes = append(bytes, chr[0])
		offset++
	}
	return bytes, int64(len(bytes)) + 1
}

func KWayMergeSort(arrBytes []int64, filename string) []int64 {
	var res MergeData
	merged := []MergeData{}
	var sortedResult []int64

	fp, _ := os.Open(filename + "_output.txt")
	defer fp.Close()
	for i := 0; i < len(arrBytes); i += 2 {
		res.StartByte = arrBytes[i]
		res.EndByte = arrBytes[i+1]
		bytes, len := readChars(fp, arrBytes[i])
		res.Len = len
		bufStr := string(bytes)
		number, err := strconv.Atoi(bufStr)
		if err != nil {
			//TODO add error
		}
		res.Number = int64(number)
		merged = append(merged, res)
	}

	var idx int
	len1 := len(merged) - 1
	for len1 >= 0 {
		for i := 0; i < len1; i++ {
			if merged[i].Number < merged[i+1].Number {
				idx = i
			} else {
				idx = i + 1
			}
		}
		sortedResult = append(sortedResult, merged[idx].Number)
		if merged[idx].StartByte+merged[idx].Len > merged[idx].EndByte {
			merged = append(merged[:idx], merged[idx+1:]...)
			len1--
		} else {
			merged[idx].StartByte += int64(merged[idx].Len)
			bytes, len := readChars(fp, merged[idx].StartByte)
			res.Len = len
			bufStr := string(bytes)
			number, err := strconv.Atoi(bufStr)
			if err != nil {
				//TODO add error
			}
			merged[idx].Number = int64(number)
		}
	}
	return sortedResult
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

		startBytes = append(startBytes, startByte)
		startByte = endByte
		startBytes = append(startBytes, endByte)
		if lastChunk {
			break
		}
	}
	//wg.Wait()
	fw.Close()

	arrMerge := KWayMergeSort(startBytes, filename)
	fmt.Println(arrMerge)
}
