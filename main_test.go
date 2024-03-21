package main

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Test_readChunk(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		input     string
		size      int
		chunk     []byte
		remainder []byte
		err       error
	}{
		"exact line": {
			input:     "foo\nbar\nbaz\n",
			size:      4,
			chunk:     []byte("foo\n"),
			remainder: []byte{},
		},
		"exact all input": {
			input:     "foo\nbar\nbaz\n",
			size:      12,
			chunk:     []byte("foo\nbar\nbaz\n"),
			remainder: []byte{},
		},
		"size bigger than input": {
			input:     "foo\nbar\nbaz\n",
			size:      13,
			chunk:     []byte("foo\nbar\nbaz\n"),
			remainder: []byte{},
		},
		"split line": {
			input:     "foo\nbar\nbaz\n",
			size:      5,
			chunk:     []byte("foo\n"),
			remainder: []byte("b"),
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := strings.NewReader(tc.input)

			buf, remainder, err := readChunk(r, tc.size)
			if diff := cmp.Diff(tc.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("unexpected error (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.chunk, buf); diff != "" {
				t.Fatalf("unexpected chunk (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.remainder, remainder); diff != "" {
				t.Fatalf("unexpected remainder (-want, +got):\n%s", diff)
			}
		})
	}
}

func Benchmark_readChunk(b *testing.B) {
	f, err := os.Open("test/measurements-10000-unique-keys.txt")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = readChunk(f, 64*1024)
		b.StopTimer()
		f.Seek(0, os.SEEK_SET)
		b.StartTimer()
	}
}

func Test_processChunk(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		chunk    []byte
		expected map[string]*TempInfo
		err      error
	}{
		"single": {
			chunk: []byte("Halifax;3.0\n"),
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   30,
					Max:   30,
					Sum:   30,
					Count: 1,
				},
			},
		},
		"multiple entries": {
			chunk: []byte("Halifax;3.0\nHalifax;2.0\nHalifax;1.0\n"),
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   30,
					Sum:   60,
					Count: 3,
				},
			},
		},
		"multiple unique city": {
			chunk: []byte("Halifax;3.0\nNew York;2.0\n"),
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   30,
					Max:   30,
					Sum:   30,
					Count: 1,
				},
				"New York": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
		},
		"multiple unique city multiple entries": {
			chunk: []byte("Halifax;3.0\nNew York;2.0\nHalifax;1.0\nNew York;5.0\nNew York;3.0\n"),
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   30,
					Sum:   40,
					Count: 2,
				},
				"New York": {
					Min:   20,
					Max:   50,
					Sum:   100,
					Count: 3,
				},
			},
		},
		"no final newline": {
			chunk: []byte("Halifax;3.0"),
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   30,
					Max:   30,
					Sum:   30,
					Count: 1,
				},
			},
		},
		"no semicolon": {
			chunk: []byte("Halifax\n"),
			err:   errInputFormat,
		},
		"empty line": {
			chunk: []byte("Halifax;2.0\n\n"),
			err:   errInputFormat,
		},
		"no number": {
			chunk: []byte("Halifax;\n"),
			err:   errInputFormat,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m, err := processChunk(tc.chunk)
			if diff := cmp.Diff(tc.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("unexpected error (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expected, m); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
	}
}

func Benchmark_processChunk_uniqueKeys(b *testing.B) {
	f, err := os.Open("test/measurements-20.txt")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	c, err := io.ReadAll(f)
	if err != nil {
		b.Fatalf("ReadAll: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = processChunk(c)
	}
}

func Benchmark_processChunk_utf8(b *testing.B) {
	f, err := os.Open("test/measurements-complex-utf8.txt")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	c, err := io.ReadAll(f)
	if err != nil {
		b.Fatalf("ReadAll: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = processChunk(c)
	}
}

func Test_mergeMap(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		left     map[string]*TempInfo
		right    map[string]*TempInfo
		expected map[string]*TempInfo
	}{
		"left empty": {
			left: map[string]*TempInfo{},
			right: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
		},
		"right empty": {
			left: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
			right: map[string]*TempInfo{},
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
		},
		"different keys": {
			left: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
			},
			right: map[string]*TempInfo{
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
				"NewYork": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
			},
		},
		"merge value": {
			left: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   20,
					Sum:   40,
					Count: 3,
				},
			},
			right: map[string]*TempInfo{
				"Halifax": {
					Min:   20,
					Max:   30,
					Sum:   50,
					Count: 2,
				},
			},
			expected: map[string]*TempInfo{
				"Halifax": {
					Min:   10,
					Max:   30,
					Sum:   90,
					Count: 5,
				},
			},
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mergeMap(tc.left, tc.right)
			if diff := cmp.Diff(tc.expected, tc.left); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
	}
}

func Test_fixRemainder(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		remainder     []byte
		chunk         []byte
		expectedLine  []byte
		expectedChunk []byte
		err           error
	}{
		"no remainder": {
			remainder:     []byte{},
			chunk:         []byte("foo;1.0\nbar;2.0\nbaz;3.0\n"),
			expectedLine:  nil,
			expectedChunk: []byte("foo;1.0\nbar;2.0\nbaz;3.0\n"),
		},
		"remainder": {
			remainder:     []byte("foo;1"),
			chunk:         []byte(".0\nbar;2.0\nbaz;3.0\n"),
			expectedLine:  []byte("foo;1.0\n"),
			expectedChunk: []byte("bar;2.0\nbaz;3.0\n"),
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			line, chunk := fixRemainder(tc.remainder, tc.chunk)
			if diff := cmp.Diff(tc.expectedLine, line); diff != "" {
				t.Fatalf("unexpected line (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expectedChunk, chunk); diff != "" {
				t.Fatalf("unexpected chunk (-want, +got):\n%s", diff)
			}
		})
	}
}

func Test_processFile(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		input    string
		size     int
		expected map[string]*TempInfo
		err      error
	}{
		"exact line": {
			input: "foo;1.0\nbar;2.0\nbaz;3.0\n",
			size:  8,
			expected: map[string]*TempInfo{
				"foo": {
					Min:   10,
					Max:   10,
					Sum:   10,
					Count: 1,
				},
				"bar": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
				"baz": {
					Min:   30,
					Max:   30,
					Sum:   30,
					Count: 1,
				},
			},
		},
		"line chunked": {
			input: "foo;1.0\nbar;2.0\nbaz;3.0\n",
			size:  12,
			expected: map[string]*TempInfo{
				"foo": {
					Min:   10,
					Max:   10,
					Sum:   10,
					Count: 1,
				},
				"bar": {
					Min:   20,
					Max:   20,
					Sum:   20,
					Count: 1,
				},
				"baz": {
					Min:   30,
					Max:   30,
					Sum:   30,
					Count: 1,
				},
			},
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := strings.NewReader(tc.input)

			m, err := processFile(r, tc.size)
			if diff := cmp.Diff(tc.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("unexpected error (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expected, m); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
	}
}

func Test_toInt(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		s string
		n int
	}{
		"zero decimal": {
			s: "5.0",
			n: 50,
		},
		"greater than 10": {
			s: "15.2",
			n: 152,
		},
		"less than 10": {
			s: "4.6",
			n: 46,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			n := toInt(tc.s)
			if diff := cmp.Diff(tc.n, n); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
	}
}
