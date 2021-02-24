package kiroku

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

const errBreak = errors.Error("break")

// New will initialize a new Kiroku instance
// Note: Processor and Options are optional
func New(dir, name string, p Processor, o *Options) (kp *Kiroku, err error) {
	// Call NewWithContext with a background context
	return NewWithContext(context.Background(), dir, name, p, o)
}

// NewWithContext will initialize a new Kiroku instance with a provided context.Context
// Note: Processor and Options are optional
func NewWithContext(ctx context.Context, dir, name string, p Processor, o *Options) (kp *Kiroku, err error) {
	var k Kiroku
	// Set output prefix
	prefix := fmt.Sprintf("Kiroku (%v)", name)
	// Initialize Kiroku output
	k.out = scribe.New(prefix)
	// Set directory as a cleaned version of the provided directory
	k.dir = filepath.Clean(dir)
	// Set name as the provided name
	k.name = name
	// Initialize cancel context with the provided context as the parent
	k.ctx, k.cancelFn = context.WithCancel(ctx)
	// Set processor
	// Note: This field is optional and might be nil
	k.p = p
	// Initialize semaphores
	k.ms = make(semaphore, 1)
	k.ps = make(semaphore, 1)

	// Check to see if options were provided
	if o != nil {
		// Options were provided, set Kiroku options as the provided options value
		k.opts = *o
	}

	// Initialize primary Chunk
	if k.c, err = newWriter(dir, name); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	// Initialize Meta
	if err = k.initMeta(); err != nil {
		err = fmt.Errorf("error initializing meta: %v", err)
		return
	}

	if !k.opts.AvoidMergeOnInit {
		// Options do not request avoiding merge on initialization, merge remaining chunks
		if err = k.handleRemaining("chunk", k.merge); err != nil {
			return
		}
	}

	if !k.opts.AvoidProcessOnInit {
		// Options do not request avoiding merge on initialization, process remaining merged chunks
		if err = k.handleRemaining("merged", k.processAndRemove); err != nil {
			return
		}
	}

	// Increment jobs waiter
	k.jobs.Add(2)
	// Initialize watch job
	go k.watch("chunk", k.ms, k.merge)
	go k.watch("merged", k.ps, k.processAndRemove)

	// Associate returning pointer to created Kiroku
	kp = &k
	return
}

// Kiroku represents historical DB entries
type Kiroku struct {
	mux sync.RWMutex

	// Output logger
	out *scribe.Scribe

	// Kiroku options
	opts Options
	// Goroutine job waiter
	jobs sync.WaitGroup

	// Last meta
	m Meta
	// Merging semaphore
	ms semaphore
	// Processing semaphore
	ps semaphore

	// Primary chunk
	c *Writer

	// Context of history
	ctx context.Context
	// Context cancel func
	cancelFn func()

	// Post processing func
	p Processor

	// Directory to store chunks
	dir string
	// Name of service
	name string
}

// Meta will return a copy of the current Meta
func (k *Kiroku) Meta() (m Meta, err error) {
	k.mux.RLock()
	defer k.mux.RUnlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		err = errors.ErrIsClosed
		return
	}

	m = k.m
	return
}

// Transaction will engage a new history transaction
func (k *Kiroku) Transaction(fn func(*Writer) error) (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	// Get current timestamp
	now := time.Now()
	// Get Unix nano value from timestamp
	unix := now.UnixNano()
	// Set name of chunk with temporary prefix
	name := fmt.Sprintf("%s.tmp.chunk.%d", k.name, unix)

	var c *Writer
	// Initialize a new chunk Writer
	if c, err = newWriter(k.dir, name); err != nil {
		return
	}

	// Since this chunk was freshly created, initialize the chunk Writer
	c.init(&k.m, unix)

	// Call provided function
	if err = fn(c); err != nil {
		// Error encountered, delete chunk!
		if deleteErr := k.deleteChunk(c); deleteErr != nil {
			// Error encountered while deleting chunk, leave error log to notify server manager
			k.out.Errorf("error deleting chunk <%s>: %v", name, deleteErr)
		}

		// Return error from provided function
		return
	}

	// Get Meta from transaction chunk
	newMeta := *c.m

	// Close transaction chunk
	if err = c.close(); err != nil {
		err = fmt.Errorf("error closing chunk: %v", err)
		return
	}

	// Rename to chunk with
	if err = k.rename(c.filename, "chunk", unix); err != nil {
		return
	}

	// Send signal to merge watcher
	k.ms.send()

	// Set underlying Meta as the transaction chunk's Meta
	k.m = newMeta
	return
}

func (k *Kiroku) Filename() (filename string, err error) {
	k.mux.RLock()
	defer k.mux.RUnlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		err = errors.ErrIsClosed
		return
	}

	filename = k.c.filename
	return
}

// Close will close the selected instance of Kiroku
func (k *Kiroku) Close() (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	// Cancel the context
	k.cancelFn()

	// Wait for jobs to finish
	k.jobs.Wait()

	var errs errors.ErrorList
	if !k.opts.AvoidMergeOnClose {
		// Options do not request avoiding merge on close, merge remaining chunks
		errs.Push(k.handleRemaining("chunk", k.merge))
	}

	if !k.opts.AvoidProcessOnClose {
		// Options do not request avoiding merge on close, process remaining merged chunks
		errs.Push(k.handleRemaining("merged", k.processAndRemove))
	}

	// Close primary chunk
	errs.Push(k.c.close())
	return errs.Err()
}

func (k *Kiroku) initMeta() (err error) {
	var (
		filename string
		ok       bool
	)

	// Get last chunk
	filename, ok, err = k.getLast("chunk")
	switch {
	case err != nil:
		// Error encountered, return
		return
	case !ok:
		// No chunks found, set Meta as the Meta from the primary chunk
		k.m = *k.c.m
		return
	default:
		// Read last chunk and set Meta from the reader
		return Read(filename, k.setMetaFromReader)
	}
}

func (k *Kiroku) setMetaFromReader(r *Reader) (err error) {
	// Set underlying Meta as the Reader's Meta
	k.m = r.Meta()
	return
}

func (k *Kiroku) getTruncatedName(filename string) (name string) {
	// Truncate name by removing directory
	// TODO: There might have been a reason it was setup this way instead of using
	// path.Base, unfortunately I forgot to leave a comment as to why I did so. This
	// is a note to do some discovery around this. The outcome should be one of two:
	//	1. Comment as to why this approach was used
	//	2. Use path.Base
	return strings.Replace(filename, k.dir+"/", "", 1)
}

func (k *Kiroku) getNext(targetPrefix string) (filename string, ok bool, err error) {
	fn := walkFn(func(iteratingName string, info os.FileInfo) (err error) {
		// Check to see if current file is a match for the current name and prefix
		if !k.isWriterMatch(targetPrefix, iteratingName, info) {
			// This is not a match, return
			return
		}

		// We found a match, set <filename> to the iterating name and set <ok> to true
		filename = iteratingName
		ok = true
		// Return break
		return errBreak
	})

	// Iterate through files within directory
	if err = filepath.Walk(k.dir, fn); err == errBreak {
		// Error was break, set to nil
		err = nil
	}

	return
}

func (k *Kiroku) getLast(targetPrefix string) (filename string, ok bool, err error) {
	fn := walkFn(func(iteratingName string, info os.FileInfo) (err error) {
		isMatch := k.isWriterMatch(targetPrefix, iteratingName, info)
		switch {
		case !isMatch && !ok:
			// We do not have a match, and we have not matched yet. Return and search
			// for more!
			return
		case !isMatch && ok:
			// We do not have a match, and we have matched before. We have exceeded
			// the range of our prefix. Return errBreak
			return errBreak

		default:
			// We found a match, set <filename> to the iterating name and set <ok> to true
			filename = iteratingName
			ok = true
			return
		}
	})

	// Iterate through files within directory
	if err = filepath.Walk(k.dir, fn); err == errBreak {
		// Error was break, set to nil
		err = nil
	}

	return
}

func (k *Kiroku) isWriterMatch(targetPrefix, filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := k.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, k.name+"."+targetPrefix) {
		// We do not have a service match, return
		return
	}

	return true
}

func (k *Kiroku) waitForNext(s semaphore) {
	select {
	// Wait for semaphore signal
	case <-s:
	// Wait for context to be finished
	case <-k.ctx.Done():
	}
}

func (k *Kiroku) sleep(d time.Duration) {
	select {
	// Wait for timer duration to complete
	case <-time.NewTimer(d).C:
	// Wait for context to be finished
	case <-k.ctx.Done():
	}
}

func (k *Kiroku) rename(filename, targetPrefix string, unix int64) (err error) {
	// Set the new name
	newName := fmt.Sprintf("%s.%s.%d", k.name, targetPrefix, unix)
	// Set filename as directory and name joined
	newFilename := path.Join(k.dir, newName)

	// Rename original filename as new filename
	if err = os.Rename(filename, newFilename); err != nil {
		err = fmt.Errorf("error renaming %s from <%s> to <%s>: %v", targetPrefix, filename, newName, err)
		return
	}

	return
}

func (k *Kiroku) watch(targetPrefix string, s semaphore, fn func(filename string) error) {
	var (
		filename string

		ok  bool
		err error
	)

	// Iterate until Kiroku is closed
	for !k.isClosed() {
		// Get next file for the target prefix
		if filename, ok, err = k.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			k.out.Errorf("error getting next %s filename: <%v>, sleeping for a minute and trying again", targetPrefix, err)
			k.sleep(time.Minute)
			continue
		}

		if !ok {
			// No match was found, wait for next signal
			k.waitForNext(s)
			continue
		}

		// Check to see if Kiroku has closed
		if k.isClosed() {
			break
		}

		// Call provided function
		if err = fn(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			k.out.Errorf("error encountered during action for <%s>: <%v>, sleeping for a minute and trying again", filename, err)
			k.sleep(time.Minute)
		}
	}

	// Decrement jobs waitgroup
	k.jobs.Done()
}

func (k *Kiroku) handleRemaining(targetPrefix string, fn func(filename string) error) (err error) {
	var (
		filename string
		ok       bool
	)

	for {
		// Get next file for the target prefix
		if filename, ok, err = k.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			err = fmt.Errorf("error getting next file for prefix <%s>: %v", targetPrefix, err)
			return
		}

		if !ok {
			return
		}

		// Call provided function
		if err = fn(filename); err != nil {
			err = fmt.Errorf("error encountered during action for <%s>: <%v>", filename, err)
			return
		}
	}
}

func (k *Kiroku) merge(filename string) (err error) {
	// Set current Unix timestamp
	unix := time.Now().UnixNano()

	// Read file and merge into primary chunk
	if err = Read(filename, k.c.merge); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	// Rename chunk to merged
	if err = k.rename(filename, "merged", unix); err != nil {
		return
	}

	// Send signal to processing semaphore
	k.ps.send()
	return
}

func (k *Kiroku) process(filename string) (err error) {
	if k.p == nil {
		// Processor not set, return
		return
	}

	// Read file and call processor
	if err = Read(filename, k.p); err != nil {
		err = fmt.Errorf("error encountered while processing: %v", err)
		return
	}

	return
}

func (k *Kiroku) processAndRemove(filename string) (err error) {
	// Process file
	if err = k.process(filename); err != nil {
		err = fmt.Errorf("error encountered while processing file <%s>: <%v>", filename, err)
		return
	}

	// Remove file
	if err = os.Remove(filename); err != nil {
		err = fmt.Errorf("error encountered while removing file <%s>: <%v>", filename, err)
		return
	}

	return
}

func (k *Kiroku) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	// Close target chunk
	errs.Push(w.close())
	// Remove target chunk
	errs.Push(os.Remove(w.filename))
	return errs.Err()
}

func (k *Kiroku) isClosed() bool {
	select {
	case <-k.ctx.Done():
		// Context done channel is closed, return true
		return true
	default:
		// Context done channel is not closed, return false
		return false
	}
}
