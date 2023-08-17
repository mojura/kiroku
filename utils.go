package kiroku

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
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
