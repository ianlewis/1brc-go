package main

import (
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

func main() {
	if len(os.Args) != 2 {
		panic("args != 2")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	// TODO: read file in chunks
	b, err := io.ReadAll(f)
	if err != nil && err != io.EOF {
		panic(err)
	}
	m, err := processChunk(b)
	if err != nil {
		panic(err)
	}

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
			// FIXME: Fix rounding error.
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

func processChunk(c []byte) (map[string]*TempInfo, error) {
	var i int
	var j int
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
