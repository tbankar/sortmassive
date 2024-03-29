package dispatcher

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
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

const (
	CHUNKSIZE = 200
)

func Error(err error) {
	if err != nil {
		log.Fatalf("Error on operation: ", err)
	}
}

func calculateRemainingBytes(fp *os.File, lastByte int64) int64 {
	chr := make([]byte, 1)
	for string(chr[0]) != "\n" {
		_, err := fp.ReadAt(chr, lastByte)
		if err == io.EOF {
			lastByte--
			for {
				fp.ReadAt(chr, lastByte)
				if string(chr[0]) == "\n" {
					return lastByte
				}
				lastByte--
			}
		}
		lastByte++
	}
	return lastByte
}

func readChars(fp *os.File, offset int64, ls byte) ([]byte, int64) {
	var bytes []byte
	chr := make([]byte, 1)
	for {
		_, eof := fp.ReadAt(chr, offset)
		if chr[0] == ls || eof == io.EOF {
			break
		}
		bytes = append(bytes, chr[0])
		offset++
	}
	return bytes, int64(len(bytes)) + 1
}

func KWayMerge(arrBytes []int64, of string, ls byte) {
	var res MergeData
	merged := []MergeData{}

	fp, err := os.OpenFile("tmp.txt", os.O_RDONLY, 0444)
	Error(err)
	defer fp.Close()

	fw, err := os.Create(of)
	Error(err)
	defer fw.Close()

	chunkArrSize := len(arrBytes)

	for i := 0; i < chunkArrSize; i += 2 {
		res.StartByte = arrBytes[i]
		res.EndByte = arrBytes[i+1]
		bytes, len := readChars(fp, arrBytes[i], ls)
		res.Len = len
		res.Number, err = strconv.ParseInt(string(bytes), 10, 64)
		Error(err)
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
		fw.WriteString(fmt.Sprintf("%d\n", merged[idx].Number))
		merged[idx].StartByte += merged[idx].Len
		bytes, bytesRead := readChars(fp, merged[idx].StartByte, ls)
		if bytesRead == 1 || merged[idx].StartByte >= merged[idx].EndByte {
			merged = append(merged[:idx], merged[idx+1:]...)
			len1--
		} else {
			merged[idx].Number, err = strconv.ParseInt(string(bytes), 10, 64)
			Error(err)
			merged[idx].Len = bytesRead
		}
	}

	// Copy remaining Bytes
	lastChunkStartidx := merged[0].StartByte
	lastChunkEndidx := merged[0].EndByte

	for lastChunkStartidx < lastChunkEndidx {
		bytes, len := readChars(fp, lastChunkStartidx, ls)
		if len == 0 {
			break
		}
		//Handle error here
		num1, _ := strconv.ParseInt(string(bytes), 10, 64)
		fw.WriteString(fmt.Sprintf("%d\n", num1))
		lastChunkStartidx += len
	}
}

func Dispatch(memory uint64) {

	config := env.GetEnvVars()
	inf := config.InputFile
	of := config.OutputFile
	ls := []byte(config.LineSeparator)[0]
	/*fi, err := os.Stat(filename)
	Error(err)
	fileSize := fi.Size()*/
	var offsets []int64
	var prevBytes int64
	offset := make(chan int64)
	done := make(chan bool)

	fr, err := os.Open(inf)
	defer fr.Close()
	Error(err)

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, CHUNKSIZE)
		return lines
	}}

	stringsPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	fwr, err := os.OpenFile("tmp.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	reader := bufio.NewReader(fr)
	for {
		buf := linesPool.Get().([]byte)
		n, err := reader.Read(buf)

		buf = buf[:n]
		if n == 0 {
			if err == io.EOF {
				break
			}
			Error(err)
		}
		nextLine, err := reader.ReadBytes(ls)
		if err != io.EOF {
			buf = append(buf, nextLine...)
		}

		wg.Add(1)
		go func() {
			worker.Run(buf, &linesPool, &stringsPool, fwr, offset, done)
			wg.Done()
		}()

	WaitTillComplete:
		for {
			select {
			case o := <-offset:
				prevBytes += o
				offsets = append(offsets, prevBytes)
			case <-done:
				break WaitTillComplete
			}
		}

		wg.Wait()
	}
	fwr.Close()
	if len(offsets) > 2 {
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		KWayMerge(offsets, of, ls)
	}
	os.Remove("tmp.txt")
}
