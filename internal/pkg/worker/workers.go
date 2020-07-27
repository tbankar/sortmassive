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
	fCounter := start
	var buffer []int
	//buffer := make([], end-start)
	mrw.RLock()
	fr.Seek(start, 0)

	/* Alternate way
	buff := make([]byte, end)
	fr.Read(buff)
	fmt.Printf("%q", string(buff))
	buf1 := strings.Split(string(buff), `\n`)
	fmt.Println(buf1)*/

	scanner := bufio.NewScanner(fr)
	for scanner.Scan() {
		if fCounter >= end {
			break
		}
		text := scanner.Text()
		fCounter = fCounter + int64(len(text)) + 1
		number, err := strconv.Atoi(text)
		if err != nil {
			continue
		}
		buffer = append(buffer, number)
	}
	sortedBuffer := sort(buffer)
	mrw.RUnlock()
	mrw.Lock()
	fw.Seek(start, 0)
	for _, num := range sortedBuffer {
		fw.WriteString(strconv.Itoa(num))
		fw.WriteString("\n")
	}
	mrw.Unlock()
}
