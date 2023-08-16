package kiroku

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gdbu/scribe"
)

func newWatcher(ctx context.Context, opts Options, out *scribe.Scribe, targetPrefix string, onTrigger func(string) error) *watcher {
	var w watcher
	w.ctx = ctx
	w.opts = opts
	w.out = out
	w.onTrigger = onTrigger

	// Initialize semaphores
	w.s = make(semaphore, 1)
	// Increment jobs waiter
	w.jobs.Add(1)
	// Initialize watch job
	go w.watch(targetPrefix)
	// Associate returning pointer to created Producer
	return &w
}

type watcher struct {
	ctx context.Context

	// Output logger
	out *scribe.Scribe

	onTrigger func(string) error

	// Merging semaphore
	s semaphore

	opts Options

	// Goroutine job waiter
	jobs sync.WaitGroup
}

func (w *watcher) watch(targetPrefix string) {
	var (
		filename string

		ok  bool
		err error
	)

	// Iterate until Producer is closed
	for !isClosed(w.ctx) {
		// Get next file for the target prefix
		if filename, ok, err = w.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			w.out.Errorf("error getting next %s filename: <%v>, sleeping for a minute and trying again", targetPrefix, err)
			w.sleep(time.Minute)
			continue
		}

		if !ok {
			// No match was found, wait for next signal
			w.waitForNext()
			continue
		}

		// Check to see if Producer has closed
		if isClosed(w.ctx) {
			break
		}

		// Call provided function
		if err = w.onTrigger(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			w.out.Errorf("error encountered during action for <%s>: <%v>, sleeping for a minute and trying again", filename, err)
			w.sleep(time.Minute)
		}
	}

	// Decrement jobs waitgroup
	w.jobs.Done()
}

func (w *watcher) getNext(targetPrefix string) (filename string, ok bool, err error) {
	fn := func(iteratingName string, info os.FileInfo) (err error) {
		// Check to see if current file is a match for the current name and prefix
		if !w.isWriterMatch(targetPrefix, iteratingName, info) {
			// This is not a match, return
			return
		}

		// We found a match, set <filename> to the iterating name and set <ok> to true
		filename = iteratingName
		ok = true
		// Return break
		return errBreak
	}

	// Iterate through files within directory
	err = walk(w.opts.Dir, fn)
	return
}

func (w *watcher) isWriterMatch(targetPrefix, filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := w.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, w.opts.FullName()+"."+targetPrefix) {
		// We do not have a service match, return
		return
	}

	return true
}

func (w *watcher) getTruncatedName(filename string) (name string) {
	// Truncate name by removing directory
	// TODO: There might have been a reason it was setup this way instead of using
	// path.Base, unfortunately I forgot to leave a comment as to why I did so. This
	// is a note to do some discovery around this. The outcome should be one of two:
	//	1. Comment as to why this approach was used
	//	2. Use path.Base
	return strings.Replace(filename, w.opts.Dir+"/", "", 1)
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
