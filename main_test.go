package main

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Test_readChunk(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		input    string
		size     int
		expected [2][]byte
		err      error
	}{
		"exact line": {
			input: "foo\nbar\nbaz\n",
			size:  4,
			expected: [2][]byte{
				[]byte("foo\n"),
				{},
			},
		},
		"exact all input": {
			input: "foo\nbar\nbaz\n",
			size:  12,
			expected: [2][]byte{
				[]byte("foo\nbar\nbaz\n"),
				{},
			},
		},
		"size bigger than input": {
			input: "foo\nbar\nbaz\n",
			size:  13,
			expected: [2][]byte{
				[]byte("foo\nbar\nbaz\n"),
				{},
			},
		},
		"split line": {
			input: "foo\nbar\nbaz\n",
			size:  5,
			expected: [2][]byte{
				[]byte("foo\n"),
				[]byte("b"),
			},
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := strings.NewReader(tc.input)

			buf, err := readChunk(r, tc.size)
			if diff := cmp.Diff(tc.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("unexpected error (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.expected, buf); diff != "" {
				t.Fatalf("unexpected result (-want, +got):\n%s", diff)
			}
		})
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
		// TODO: Maybe ignore empty lines?
		"empty line": {
			chunk: []byte("Halifax;2.0\n\n"),
			err:   errInputFormat,
		},
		"bad number format": {
			chunk: []byte("Halifax;abc\n"),
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
