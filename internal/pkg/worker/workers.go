package worker

import (
	"fmt"
	"os"
	"strconv"
	"sync"
)

func merge(left []int, right []int) []int {
	var result []int
	for len(left) != 0 && len(right) != 0 {
		if left[0] < right[0] {
			result = append(result, left[0])
			left = left[1:]
		} else {
			result = append(result, right[0])
			left = right[1:]
		}
	}
	result = append(result, left...)
	result = append(result, right...)
	return result
}

func sort(data []int) []int {
	if len(data) < 2 {
		return data
	}
	mid := len(data) / 2
	left := data[0:mid]
	right := data[mid:]
	return merge(sort(left), sort(right))
}

func byteToInt(buff []byte) []int {
	var res []int
	var str string
	var i, j int

	for ; i < len(buff); i++ {
		for j = i; string(buff[j]) != "\n"; j, i = j+1, i+1 {
			str = str + string(buff[i])
		}
		t, err := strconv.Atoi(str)
		//TODO need to handle err
		if err == nil {
			res = append(res, t)
		}
		str = ""
	}
	return res
}

func Run(start, end int64, fr *os.File, fw *os.File, wg *sync.WaitGroup, mrw *sync.RWMutex) {
	defer wg.Done()
	buffer := make([]byte, end-start)
	mrw.RLock()
	fr.Seek(start, 0)
	fr.Read(buffer)
	mrw.RUnlock()
	intBuff := byteToInt(buffer)
	sortedBuffer := sort(intBuff)
	fmt.Println(sortedBuffer)
	//Start := start
	mrw.Lock()
	//fw.WriteAt([]byte(sortedBuffer), Start)
	mrw.Unlock()
}
