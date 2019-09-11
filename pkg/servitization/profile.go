package servitization

import (
	"os"
	"runtime/pprof"
)

var (
	cpuProfFile          *os.File
	memProfFile          *os.File
	blockProfFile        *os.File
	goroutineProfFile    *os.File
	threadcreateProfFile *os.File
)

func startProfiling() {
	cpuProfFile, _ = os.Create("cpu.prof")
	memProfFile, _ = os.Create("memory.prof")
	blockProfFile, _ = os.Create("block.prof")
	goroutineProfFile, _ = os.Create("goroutine.prof")
	threadcreateProfFile, _ = os.Create("threadcreate.prof")

	pprof.StartCPUProfile(cpuProfFile)
}

func saveProfiling() {
	goroutine := pprof.Lookup("goroutine")
	goroutine.WriteTo(goroutineProfFile, 1)

	heap := pprof.Lookup("heap")
	heap.WriteTo(memProfFile, 1)

	block := pprof.Lookup("block")
	block.WriteTo(blockProfFile, 1)

	threadcreate := pprof.Lookup("threadcreate")
	threadcreate.WriteTo(threadcreateProfFile, 1)

	pprof.StopCPUProfile()
}
