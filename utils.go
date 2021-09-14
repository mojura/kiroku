package kiroku

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func walk(dir string, fn func(string, os.FileInfo) error) (err error) {
	wfn := func(filename string, info os.FileInfo, ierr error) (err error) {
		switch {
		case ierr == nil:
			// Call provided function
			return fn(filename, info)
		case ierr != nil && filename == dir:
			// We've encountered an error with the target directory, return iterating error
			return ierr
		default:
			return
		}
	}

	// Iterate through files within directory
	if err = filepath.Walk(dir, wfn); err == errBreak {
		// Error was break, set to nil
		err = nil
	}

	return
}

func generateFilename(name string, unixNano int64) string {
	return fmt.Sprintf("%s.%d.moj", name, unixNano)
}

func removeFile(f fs.File, dir string) (err error) {
	var info fs.FileInfo
	if info, err = f.Stat(); err != nil {
		return
	}

	filename := filepath.Join(dir, info.Name())

	if err = f.Close(); err != nil {
		return
	}

	return os.Remove(filename)
}

type File interface {
	io.Seeker
	io.Reader
	io.ReaderAt
}
