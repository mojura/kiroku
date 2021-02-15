package kiroku

import (
	"os"
	"path/filepath"
)

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
