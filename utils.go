package kiroku

import (
	"os"
	"path/filepath"
)

// Read will read a filename and provide a temporary reader
func Read(filename string, p Processor) (err error) {
	var f *os.File
	// Open file with provided filename
	if f, err = os.Open(filename); err != nil {
		// Error opening file, return
		return
	}
	// Close file whenever the function ends
	defer f.Close()

	var r *Reader
	// Initialize a new Reader utilizing the recently opened file
	if r, err = NewReader(f); err != nil {
		return
	}

	// Call provided Processor and return whatever error it produces
	return p(r)
}

// walkFn is a helper function to make iterating through files slightly more pleasant
func walkFn(fn func(string, os.FileInfo) error) filepath.WalkFunc {
	return func(filename string, info os.FileInfo, ierr error) (err error) {
		if ierr != nil {
			// Iterating error exists, return
			return
		}

		// Call provided function
		return fn(filename, info)
	}
}
