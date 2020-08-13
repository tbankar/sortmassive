package worker

import (
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	WCHUNKSZ = 100
)

func merge(left, right []int64) []int64 {
	var result []int64
	for len(left) != 0 && len(right) != 0 {
		if left[0] < right[0] {
			result = append(result, left[0])
			left = left[1:]
		} else {
			result = append(result, right[0])
			right = right[1:]
		}
	}
	result = append(result, left...)
	result = append(result, right...)
	return result
}

func sortData(data []int64) []int64 {
	if len(data) == 1 {
		return data
	}
	mid := len(data) / 2
	left := data[0:mid]
	right := data[mid:]
	return merge(sortData(left), sortData(right))
}

func convertToInt64(buffer []byte) []int64 {
	var arr []int64
	for i := 0; i < len(buffer); i++ {
		str := ""
		for i < len(buffer) && buffer[i] != '\n' {
			str = str + string(buffer[i])
			i++
		}
		num, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			//handle error here
		}
		arr = append(arr, num)
	}
	return arr
}

func writeToFile(buff []int64, offset int64, fp *os.File, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	fp.Seek(0, 2)
	for _, num := range buff {
		str := strconv.FormatInt(num, 10)
		//Handle error
		n, _ := fp.Write([]byte(str + "\n"))
		if n < 1 {
			break
		}
	}
}

func Run(chunk []byte, linesPool, stringsPool *sync.Pool, fp *os.File, offset chan<- int64, done chan<- bool) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	linesPool.Put(chunk)

	str := stringsPool.Get().(string)
	str = string(chunk)
	buf := strings.Split(str, "\n")
	stringsPool.Put(str)

	n := len(buf)

	var workers int
	if WCHUNKSZ > n {
		workers = 1
	} else {
		workers = n / WCHUNKSZ

		if workers%WCHUNKSZ != 0 {
			workers++
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {

		go func(start, end int64) {
			defer wg.Done()
			var numBuff []int64
			var totalBytes int
			for i := start; i < end; i++ {
				//Handle error here
				num, err := strconv.ParseInt(buf[i], 10, 64)
				if err != nil {
					continue
				}
				totalBytes += len(buf[i]) + 1
				numBuff = append(numBuff, num)
			}
			sortedBuffer := sortData(numBuff)
			writeToFile(sortedBuffer, int64(start), fp, &mu)
			offset <- start
			offset <- int64(totalBytes)

		}(int64(i*WCHUNKSZ), int64(math.Min(float64((i+1)*WCHUNKSZ), float64(n))))
	}
	wg.Wait()
	buf = nil
	done <- true
}

/*func Run(start, end int64, fr, fw *os.File, wg *sync.WaitGroup, mrw *sync.RWMutex) {
	defer wg.Done()
	mrw.RLock()
	fr.Seek(start, 0)

	buff := make([]byte, end-start)
	//Handle error here
	bytes, _ := fr.Read(buff)
	mrw.RUnlock()
	if bytes < 1 {
		return
	}
	buffOfInts := convertToInt64(buff)

	sortedBuffer := sortData(buffOfInts)
	mrw.Lock()
	fw.Seek(start, 0)
	for _, num := range sortedBuffer {
		str := strconv.FormatInt(num, 10)
		b := []byte(str + "\n")
		n, err := fw.Write(b)
		if err != nil {
			//Handle error
		}
		if n < 1 {
			break
		}
	}
	mrw.Unlock()
}*/
