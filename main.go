package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	// "strconv"
)

// TempInfo stores temperature stats for a single city.
type TempInfo struct {
	Min   int
	Sum   int
	Count int
	Max   int
}

var (
	errInputFormat = errors.New("bad input format")

	cpuprofile       = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile       = flag.String("memprofile", "", "write memory profile to `file`")
	executionprofile = flag.String("execprofile", "", "write trace execution to `file`")
	input            = flag.String("input", "", "path to the input file to evaluate")
)

func main() {
	flag.Parse()

	if *executionprofile != "" {
		f, err := os.Create(*executionprofile)
		if err != nil {
			log.Fatal("could not create trace execution profile: ", err)
		}
		defer f.Close()
		trace.Start(f)
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	args := flag.Args()
	if len(args) != 1 {
		log.Fatalf("invalid arguments: %v", args)
	}
	f, err := os.Open(args[0])
	if err != nil {
		log.Fatal(err)
	}

	m, err := processFile(f, 64*1024*1024)
	if err != nil {
		log.Fatal(err)
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	printMap(m)
}

// round rounds to the nearest tenth.
func round(n float64) float64 {
	r := math.Round(n * 10)
	if r == -0.0 {
		return 0.0
	}
	return r / 10
}

func printMap(m map[string]*TempInfo) {
	// Print the output alphabetically.
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Print("{")
	for i, k := range keys {
		v := m[k]
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f",
			k,
			round(float64(v.Min))/10,
			round(float64(v.Sum)/10/float64(v.Count)),
			round(float64(v.Max))/10,
		)
		if i != len(keys)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Print("}\n")
}

func readChunks(r io.Reader, chunkSize int, chunkChan chan []byte, errChan chan error) {
	defer close(chunkChan)
	var remainder []byte
	for {
		chunkRead, nextRemainder, readErr := readChunk(r, chunkSize)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			errChan <- readErr
			return
		}

		firstLine, chunk := fixRemainder(remainder, chunkRead)
		if len(firstLine) > 0 {
			chunkChan <- firstLine
		}
		if len(chunk) > 0 {
			chunkChan <- chunk
		}

		remainder = nextRemainder
		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	// Handle the remainder if there is one.
	if len(remainder) > 0 {
		chunkChan <- remainder
	}
}

func processChunks(chunkChan chan []byte, mapChan chan map[string]*TempInfo, errChan chan error, doneChan chan struct{}) {
	defer func() {
		doneChan <- struct{}{}
	}()

	for chunk := range chunkChan {
		m, err := processChunk(chunk)
		if err != nil {
			errChan <- err
			return
		}
		mapChan <- m
	}
}

func processFile(r io.Reader, chunkSize int) (map[string]*TempInfo, error) {
	// Create 1 goroutine per CPU core.
	// 1: read chunks from file and send to chunkChan
	// N-2: read chunks from chunkChan, process and send result to mapChan
	// main: read results from mapChan and merge.

	numCPU := runtime.NumCPU()
	processGoroutines := numCPU

	chunkChan := make(chan []byte, processGoroutines*3)
	mapChan := make(chan map[string]*TempInfo, processGoroutines*2)
	errChan := make(chan error, processGoroutines)

	go readChunks(r, chunkSize, chunkChan, errChan)

	doneChan := make(chan struct{}, numCPU-2)
	for i := 0; i < processGoroutines; i++ {
		go processChunks(chunkChan, mapChan, errChan, doneChan)
	}

	// Wait for chunks to be processed.
	tempMap := map[string]*TempInfo{}

	// Merge maps and stop when doneChan recieves all values.
	var i int
	for {
		select {
		case m := <-mapChan:
			mergeMap(tempMap, m)
		case <-doneChan:
			i++
		}
		if i >= processGoroutines {
			// All processors are done. Drain the map channel.
			for len(mapChan) > 0 {
				mergeMap(tempMap, <-mapChan)
			}
			break
		}
	}

	// Return an error if there is one.
	select {
	case err := <-errChan:
		return nil, err
	default:
		return tempMap, nil
	}
}

// readChunk reads and returns two chunks of input totaling the given size.
// The first chunk contains full lines that can be processed. The second chunk is
// the remainder which is a partial line. This is done to avoid copies.
func readChunk(r io.Reader, size int) ([]byte, []byte, error) {
	buf := make([]byte, size)          // lines of input
	remainder := make([]byte, 0, size) // remaining bytes
	bytesRead, err := r.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return buf, remainder, err
	}
	buf = buf[:bytesRead]

	i := bytes.LastIndexByte(buf, '\n')
	remainder = buf[i+1 : bytesRead]
	buf = buf[:i+1]

	return buf, remainder, err
}

// fixRemainder creates a new bytes slice for the first line from a remainder
// of a previous chunk and the begining of the next chunk. It returns the first
// line and remainder of the next chunk. This is so that only the first line
// need be copied.
func fixRemainder(remainder, chunk []byte) ([]byte, []byte) {
	if chunk == nil {
		panic("nil chunk")
	}

	var firstLine []byte
	var nextChunk []byte
	if len(remainder) == 0 {
		nextChunk = chunk
	} else {
		// Handle the first line.
		nlIndex := bytes.IndexByte(chunk, '\n')
		firstLine = append(firstLine, remainder...)
		if nlIndex != -1 {
			firstLine = append(firstLine, chunk[:nlIndex+1]...)
			nextChunk = chunk[nlIndex+1:]
		} else {
			firstLine = append(firstLine, chunk...)
		}
	}
	return firstLine, nextChunk
}

// processChunk reads an input chunk. Chunks should be comprised of full lines.
func processChunk(c []byte) (map[string]*TempInfo, error) {
	m := make(map[string]*TempInfo)
	if c == nil {
		return m, nil
	}

	var i int // index into chunk.
	var j int // start index used for parsing.
	for {
		// Read name
		var name string
		j = i
		for {
			if i >= len(c) {
				return nil, fmt.Errorf("%w: unexpected end of input", errInputFormat)
			}
			if c[i] == ';' {
				name = string(c[j:i])
				i++
				break
			}
			i++
		}

		// Read num
		j = i
		for {
			if i >= len(c) || c[i] == '\n' {
				if len(c[j:i]) == 0 {
					return nil, fmt.Errorf("%w: unexpected end of input", errInputFormat)
				}
				num := toInt(c[j:i])

				if info, ok := m[name]; ok {
					if num < info.Min {
						info.Min = num
					}
					info.Sum += num
					if num > info.Max {
						info.Max = num
					}
					info.Count++
				} else {
					m[name] = &TempInfo{
						Min:   num,
						Sum:   num,
						Max:   num,
						Count: 1,
					}
				}

				i++
				break
			}
			i++
		}

		if i >= len(c) {
			return m, nil
		}
	}
}

// mergeMap merges the right map into the left map.
func mergeMap(left, right map[string]*TempInfo) {
	for k := range right {
		rInfo := right[k]
		if lInfo, ok := left[k]; ok {
			if rInfo.Min < lInfo.Min {
				lInfo.Min = rInfo.Min
			}
			if rInfo.Max > lInfo.Max {
				lInfo.Max = rInfo.Max
			}
			lInfo.Sum += rInfo.Sum
			lInfo.Count += rInfo.Count
		} else {
			left[k] = right[k]
		}
	}
}

func toInt(b []byte) int {
	var isNegative bool
	if b[0] == '-' {
		isNegative = true
		b = b[1:]
	}

	var n int
	for i := range b {
		if b[i] != '.' {
			n *= 10
			n += int(b[i] - '0')
		}
	}

	if isNegative {
		n *= -1
	}
	return n
}
