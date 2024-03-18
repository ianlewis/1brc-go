package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
)

type tempInfo struct {
	min   int
	sum   int
	count int
	max   int
}

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
	m := processChunk(b)

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
			round(float64(v.min))/10,
			// FIXME: Fix rounding error.
			round(float64(v.sum/10/v.count)),
			round(float64(v.max))/10,
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

func processChunk(c []byte) map[string]*tempInfo {
	var i int
	var j int
	m := make(map[string]*tempInfo)
	for {
		// Read name
		// FIXME: handle utf8 names.
		var name string
		j = i
		for {
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
			if c[i] == '\n' {
				// TODO: Parse the number into an int.
				fnum, err = strconv.ParseFloat(string(c[j:i]), 64)
				if err != nil {
					panic(err)
				}
				num = int(fnum * 10)

				if info, ok := m[name]; ok {
					if num < info.min {
						info.min = num
					}
					info.sum += num
					if num > info.max {
						info.max = num
					}
					info.count++
				} else {
					m[name] = &tempInfo{
						min:   num,
						sum:   num,
						max:   num,
						count: 1,
					}
				}

				i++
				break
			}
			i++
		}

		if i >= len(c) {
			return m
		}
	}
}
