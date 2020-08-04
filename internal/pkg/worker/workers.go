package worker

import (
	"os"
	"strconv"
	"sync"
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

func Run(start, end int64, fr, fw *os.File, wg *sync.WaitGroup, mrw *sync.RWMutex) {
	defer wg.Done()
	//var buffer []int64
	mrw.Lock()
	fr.Seek(start, 0)

	buff := make([]byte, end-start)
	//Handle error here
	bytes, _ := fr.Read(buff)
	mrw.Unlock()
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
			//Validate error
		}
		if n < 1 {
			break
		}
	}
	mrw.Unlock()
}
