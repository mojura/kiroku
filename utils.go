package kiroku

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

func GenerateFilename(name, kind string, timestamp int64) string {
	if timestamp == 0 {
		return ""
	}

	return fmt.Sprintf("%s.%d.%s.moj", name, timestamp, kind)

}

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

func parseFilename(filename string) (parsed filenameMeta, err error) {
	spl := strings.Split(filename, ".")
	if len(spl) != 4 {
		err = fmt.Errorf("invalid number of filename parts, expected 4 and received %d", len(spl))
		return
	}

	if parsed.createdAt, err = strconv.ParseInt(spl[1], 10, 64); err != nil {
		return
	}

	parsed.name = spl[0]
	parsed.kind = spl[2]
	return
}

type filenameMeta struct {
	name      string
	kind      string
	createdAt int64
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

func getSnapshotName(name string) string {
	return fmt.Sprintf("_latestSnapshots/%s.txt", name)
}

func isNilSource(s Source) (isNil bool) {
	val := reflect.ValueOf(s)
	if !val.IsValid() {
		return true
	}

	if val.IsZero() {
		return true
	}

	if val.IsNil() {
		return true
	}

	return false
}
