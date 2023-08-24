package kiroku

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/edsrzf/mmap-go"
)

// This block is for aliases of common OS operations. They are setup as aliases
// so they can be easily mocked for testing purposes.
var (
	createFile       = os.Create
	createAppendFile = func(filepath string) (f *os.File, err error) {
		return os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0744)
	}
	renameFile = os.Rename
	mapRegion  = mmap.MapRegion
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

func getSnapshotName(name string) string {
	return fmt.Sprintf("%s.txt", name)
}

func isNilSource(s Source) (isNil bool) {
	val := reflect.ValueOf(s)
	if !val.IsValid() {
		return true
	}

	if val.IsZero() {
		return true
	}

	return false
}

func isClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		// Context done channel is closed, return true
		return true
	default:
		// Context done channel is not closed, return false
		return false
	}
}

func sleep(ctx context.Context, sleepDuration time.Duration) (err error) {
	timer := time.NewTimer(sleepDuration)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}

	return
}

func wasCreatedAfter(filename string, timestamp int64) (after bool, err error) {
	var parsed Filename
	if parsed, err = parseFilename(filename); err != nil {
		return
	}

	return timestamp < parsed.createdAt, nil
}

func handleTwoErrors(a, b error) (err error) {
	switch {
	case a != nil:
		return a
	case b != nil:
		return b
	default:
		return nil
	}
}
