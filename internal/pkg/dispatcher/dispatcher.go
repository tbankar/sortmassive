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
		_, eof := fp.ReadAt(chr, offset)
		if chr[0] == '\n' || eof == io.EOF {
			break
		}
		bytes = append(bytes, chr[0])
		offset++
	}
	return bytes, int64(len(bytes)) + 1
}

func KWayMergeSort(arrBytes []int64, filename string) {
	var res MergeData
	merged := []MergeData{}

	fp, _ := os.Open(filename + "_output.txt")
	defer fp.Close()
	fw1, _ := os.OpenFile(filename+"_output1.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	defer fw1.Close()
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

	len1 := len(merged) - 1
	for len1 > 0 {
		idx := 0
		for i := 0; i < len1; i++ {
			if merged[idx].Number > merged[i+1].Number {
				idx = i + 1
			}
		}
		fw1.WriteString(fmt.Sprintf("%d\n", merged[idx].Number))
		merged[idx].StartByte += merged[idx].Len
		bytes, bytesRead := readChars(fp, merged[idx].StartByte)
		if bytesRead == 1 || merged[idx].StartByte >= merged[idx].EndByte {
			merged = append(merged[:idx], merged[idx+1:]...)
			len1--
		} else {
			bufStr := string(bytes)
			number, err := strconv.Atoi(bufStr)
			if err != nil {
				//TODO add error
			}
			merged[idx].Number = int64(number)
			merged[idx].Len = bytesRead
		}
	}

	// Copy remaining Bytes
	for merged[0].StartByte < merged[0].EndByte {
		bytes, len := readChars(fp, merged[0].StartByte)
		if len == 0 {
			break
		}
		bufStr, _ := strconv.Atoi(string(bytes))
		merged[0].Number = int64(bufStr)
		merged[0].Len = len
		fw1.WriteString(fmt.Sprintf("%d\n", merged[0].Number))
		merged[0].StartByte += merged[0].Len
	}
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
	var endByte int64
	var lastChunk bool
	for ; workers > 0; workers-- {
		endByte = startByte + chunkSize
		addBytes := calculateRemainingBytes(fr, endByte)
		if endByte > addBytes {
			lastChunk = true
		}
		endByte = addBytes

		if endByte < startByte {
			break
		}
		go worker.Run(startByte, endByte, fr, fw, &wg, &mrw)

		startBytes = append(startBytes, startByte)
		startByte = endByte
		startBytes = append(startBytes, endByte)
		if lastChunk {
			break
		}
	}
	wg.Wait()

	KWayMergeSort(startBytes, filename)
}
