package main

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	segmentSize = 2 * 1024 * 1024 // 2mb
)

// processChunksRandom reads chunks, processes each line in the chunk,
// and sends the resulting map for the chunk to mapChan. If any errors occur,
// the error is sent to errChan and processChunks returns immediately.
func processChunksRandom(data []byte, cursor *atomic.Int64, size int64, mapChan chan map[string]*TempInfo, errChan chan error, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	for {
		offset := cursor.Add(size) - size
		end := offset + size
		fileLength := int64(len(data))
		if offset >= fileLength {
			return
		}
		if end > fileLength {
			end = fileLength
		}

		if offset != 0 && data[offset-1] != '\n' {
			// Start after the first newline.
			nlOffset := int64(bytes.IndexByte(data[offset:end], '\n'))
			if nlOffset < 0 {
				return
			}
			offset += nlOffset + 1
		}
		if data[end-1] != '\n' {
			// Process the partial line at the end.
			nlOffset := int64(bytes.IndexByte(data[end:], '\n'))
			if nlOffset < 0 {
				end = fileLength
			} else {
				end += nlOffset + 1
			}

		}

		m, err := processChunk(data[offset:end])
		if err != nil {
			errChan <- err
			return
		}
		mapChan <- m
	}
}

// processFileRandom reads the file at path in segments of size and produces a
// resulting map for the entire file.
func processFileRandom(path string, size int) (map[string]*TempInfo, error) {
	// Create 1 goroutine per CPU core.
	// 1: read chunks from file and send to chunkChan
	// N-2: read chunks from chunkChan, process and send result to mapChan
	// main: read results from mapChan and merge.
	processGoroutines := runtime.NumCPU()
	var cursor atomic.Int64

	mapChan := make(chan map[string]*TempInfo, processGoroutines*2)
	errChan := make(chan error, processGoroutines)

	f, data, err := mmapFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	defer munmap(data)

	var wg sync.WaitGroup
	for i := 0; i < processGoroutines; i++ {
		wg.Add(1)
		go processChunksRandom(data, &cursor, int64(size), mapChan, errChan, &wg)
	}

	// Wait until all goroutines are finished and close the map channel.
	go func() {
		wg.Wait()
		close(mapChan)
	}()

	// Merge resulting maps
	tempMap := make(map[string]*TempInfo, maxCities)
	for m := range mapChan {
		mergeMap(tempMap, m)
	}

	// Return an error if there is one.
	select {
	case err := <-errChan:
		return nil, err
	default:
		return tempMap, nil
	}
}
