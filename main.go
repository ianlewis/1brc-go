package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
)

// TempInfo stores temperature stats for a single city.
type TempInfo struct {
	Min   int
	Sum   int
	Count int
	Max   int
}

var errInputFormat = errors.New("bad input format")

// TODO: remove panics.

func main() {
	if len(os.Args) != 2 {
		panic("args != 2")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	// TODO: Pick a good chunk size.
	m, err := processFile(f, 64*1024)
	if err != nil {
		panic(err)
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

	// TODO: Pick the right buffer sizes for channels.
	chunkChan := make(chan []byte, 5)
	mapChan := make(chan map[string]*TempInfo, 5)
	errChan := make(chan error, 5)

	numCPU := runtime.NumCPU()

	go readChunks(r, chunkSize, chunkChan, errChan)

	doneChan := make(chan struct{}, numCPU-2)
	for i := 0; i < numCPU-2; i++ {
		go processChunks(chunkChan, mapChan, errChan, doneChan)
	}

	// Wait for chunks to be processed.
	tempMap := map[string]*TempInfo{}

	// TODO: merge maps and stop when chunkChan is closed and doneChan recieves all values.

	return tempMap, nil
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
		var fnum float64
		var num int
		var err error
		for {
			if i >= len(c) || c[i] == '\n' {
				// TODO: Parse the number into an int.
				fnum, err = strconv.ParseFloat(string(c[j:i]), 64)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", errInputFormat, err)
				}
				num = int(fnum * 10)

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
