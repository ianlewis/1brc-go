package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
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

	// Read file in chunks
	var tempMap map[string]*TempInfo
	for {
		c, err := readChunk(f, 64*1024)
		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
		// TODO: handle remaining bytes from readChunk.
		m, err := processChunk(c[0])
		if err != nil {
			panic(err)
		}
		mergeMap(tempMap, m)
		if errors.Is(err, io.EOF) {
			break
		}
	}

	// Print the output alphabetically.
	var keys []string
	for k := range tempMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Print("{")
	for i, k := range keys {
		v := tempMap[k]
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f",
			k,
			round(float64(v.Min))/10,
			round(float64(v.Sum/10/v.Count)),
			round(float64(v.Max))/10,
		)
		if i != len(keys)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Print("}\n")
}

// round rounds to the nearest tenth.
func round(n float64) float64 {
	r := math.Round(n * 10)
	if r == -0.0 {
		return 0.0
	}
	return r / 10
}

// readChunk reads and returns two chunks of input totaling the given
// size. This is done to avoid copies.
func readChunk(r io.Reader, size int) ([2][]byte, error) {
	buf := [2][]byte{
		make([]byte, size),    // lines of input
		make([]byte, 0, size), // remaining bytes
	}
	bytesRead, err := r.Read(buf[0])
	if err != nil && !errors.Is(err, io.EOF) {
		return buf, err
	}
	buf[0] = buf[0][:bytesRead]

	i := bytes.LastIndex(buf[0], []byte{'\n'})
	buf[1] = buf[0][i+1 : bytesRead]
	buf[0] = buf[0][:i+1]

	return buf, nil
}

// processChunk reads an input chunk.
func processChunk(c []byte) (map[string]*TempInfo, error) {
	var i int // index into chunk.
	var j int // start index used for parsing.
	m := make(map[string]*TempInfo)
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
