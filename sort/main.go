package main

import (
	"C"
	"log"
	"runtime"
	"sortmassive/internal/pkg/dispatcher"
	"syscall"
)

func main() {
	var RAM uint64
	stats := &syscall.Sysinfo_t{}
	err := syscall.Sysinfo(stats)
	if err != nil {
		log.Fatalf("%s", err)
	}
	RAM = (stats.Totalram) / 2
	runtime.GOMAXPROCS(0)
	dispatcher.Dispatch(RAM)
}
