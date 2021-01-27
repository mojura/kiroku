package history

import (
	"io"
	"os"
	"path/filepath"
)

func newReader(f *os.File) (r *io.SectionReader, err error) {
	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		return
	}

	r = io.NewSectionReader(f, metaSize, info.Size()-metaSize)
	return
}

func walkFn(fn func(string, os.FileInfo) error) filepath.WalkFunc {
	return func(filename string, info os.FileInfo, ierr error) (err error) {
		if ierr != nil {
			return
		}

		return fn(filename, info)
	}
}

// Read will read a filename and provide a temporary reader
func Read(filename string, fn func(*Reader) error) (err error) {
	var f *os.File
	if f, err = os.Open(filename); err != nil {
		return
	}
	defer f.Close()

	var r *Reader
	if r, err = NewReader(f); err != nil {
		return
	}

	return fn(r)
}
