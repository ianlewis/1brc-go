//go:build darwin || dragonfly || freebsd || linux || openbsd || solaris || netbsd

package main

import (
	"fmt"
	"os"
	"syscall"
)

// mmapFile returns a read-only mmaped byte slice to the given file path.
func mmapFile(path string) (*os.File, []byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("mmap: could not open %q: %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("mmap: could not stat %q: %w", path, err)
	}

	size := fi.Size()
	if size == 0 {
		return f, []byte{}, nil
	}
	if size < 0 {
		f.Close()
		return nil, nil, fmt.Errorf("mmap: file %q has negative size", path)
	}
	if size != int64(int(size)) {
		f.Close()
		return nil, nil, fmt.Errorf("mmap: file %q is too large", path)
	}

	b, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, b, err
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}
