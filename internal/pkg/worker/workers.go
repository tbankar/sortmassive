package worker

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"
)

func merge(left, right []int) []int {
	var result []int
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

func sort(data []int) []int {
	if len(data) == 1 {
		fmt.Println(data)
		return data
	}
	mid := len(data) / 2
	left := data[0:mid]
	right := data[mid:]
	return merge(sort(left), sort(right))
}

func Run(start, end int64, fr, fw *os.File, wg *sync.WaitGroup, mrw *sync.RWMutex) {
	defer wg.Done()
	var buffer []int
	mrw.RLock()
	fr.Seek(start, 0)
	scanner := bufio.NewScanner(fr)
	for scanner.Scan() {
		if start >= end {
			break
		}
		text := scanner.Text()
		start += int64(len(text))
		number, err := strconv.Atoi(text)
		if err != nil {
			continue
		}
		buffer = append(buffer, number)
	}
	sortedBuffer := sort(buffer)
	fmt.Println(sortedBuffer)
	mrw.RUnlock()
	//Start := start
	mrw.Lock()
	//fw.WriteAt([]byte(sortedBuffer), Start)
	mrw.Unlock()
}
