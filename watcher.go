package kiroku

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func newWatcher(ctx context.Context, opts Options, onTrigger func(Filename) error, ts ...Type) *watcher {
	var w watcher
	w.ctx = ctx
	w.opts = opts
	w.onTrigger = onTrigger

	// Initialize semaphores
	w.s = make(semaphore, 1)
	// Set types
	w.ts = ts
	// Increment jobs waiter
	w.jobs.Add(1)

	// Initialize watch job
	go w.watch()
	// Associate returning pointer to created Producer
	return &w
}

type watcher struct {
	ctx context.Context

	onTrigger func(Filename) error

	// Merging semaphore
	s semaphore
	// Types
	ts []Type

	opts Options

	// Goroutine job waiter
	jobs sync.WaitGroup
}

func (w *watcher) watch() {
	var (
		ok  bool
		err error
	)

	// Decrement jobs waitgroup when func is done
	defer w.jobs.Done()
	// Iterate until Producer is closed
	for !isClosed(w.ctx) {
		if ok, err = w.process(); err != nil {
			err = fmt.Errorf("error processing: %v", err)
			w.opts.OnError(err)
			w.sleep(time.Minute)
		}

		if !ok {
			w.waitForNext()
		}
	}
}

func (w *watcher) processAll() (err error) {
	var ok bool
	// Iterate until Producer is closed
	for {
		if ok, err = w.process(); !ok || err != nil {
			return
		}
	}
}

// process will process matches until:
//   - No more matches are found
//   - Watcher has been closed
func (w *watcher) process() (ok bool, err error) {
	var filename Filename
	// Get next file for the target prefix
	if filename, ok, err = w.getNext(); err != nil {
		err = fmt.Errorf("error getting next %+v filename: <%v>, sleeping for a minute and trying again", w.ts, err)
		return
	}

	if !ok {
		return
	}

	// Call provided function
	if err = w.onTrigger(filename); err != nil {
		err = fmt.Errorf("error encountered during action for <%s>: <%v>, sleeping for a minute and trying again", filename, err)
		return
	}

	return
}

func (w *watcher) getNext() (filename Filename, ok bool, err error) {
	cleanDir := filepath.Clean(w.opts.Dir)
	fn := func(iteratingName string, info os.FileInfo) (err error) {
		if info.IsDir() {
			// We are not interested in directories, return
			return
		}

		if filepath.Dir(iteratingName) != cleanDir {
			// Current item is not in the same directory, return
			return
		}

		truncated := filepath.Base(iteratingName)
		// Check to see if current file is a match for the current name and prefix
		if filename, err = ParseFilename(truncated); err != nil {
			err = nil
			return
		}

		if filename.name != w.opts.FullName() {
			return
		}

		for _, t := range w.ts {
			if filename.filetype == t {
				ok = true
				return errBreak
			}
		}

		return
	}

	// Iterate through files within directory
	if err = walk(w.opts.Dir, fn); err == errBreak {
		err = nil
	}

	return
}

func (w *watcher) waitForNext() {
	select {
	// Wait for semaphore signal
	case <-w.s:
	// Wait for context to be finished
	case <-w.ctx.Done():
	}
}

func (w *watcher) sleep(d time.Duration) {
	select {
	// Wait for timer duration to complete
	case <-time.NewTimer(d).C:
	// Wait for context to be finished
	case <-w.ctx.Done():
	}
}

func (w *watcher) waitToComplete() {
	w.jobs.Wait()
}

func (w *watcher) trigger() {
	w.s.send()
}
