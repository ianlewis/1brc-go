package main

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Test_processFileRandom(t *testing.T) {
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

			f, err := os.CreateTemp("", "")
			if err != nil {
				t.Fatalf("unable to create temporary file: %v", err)
			}
			_, err = f.WriteString(tc.input)
			if err != nil {
				t.Fatalf("unable to write temporary file: %v", err)
			}
			f.Close()
			// defer os.Remove(f.Name())

			m, err := processFileRandom(f.Name(), tc.size)
			if diff := cmp.Diff(tc.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("unexpected error (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expected, m); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
	}
}

func Benchmark_processFileRandom(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = processFileRandom("test/measurements-10000-unique-keys.txt", segmentSize)
	}
}
