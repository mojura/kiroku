package history

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

const errBreak = errors.Error("break")

// New will initialize a new Kiroku instance
// Note: PostProcessor is optional
func New(dir, name string, pp Processor) (kp *Kiroku, err error) {
	var k Kiroku
	prefix := fmt.Sprintf("Mojura history (%v)", name)
	k.out = scribe.New(prefix)
	k.dir = filepath.Clean(dir)
	k.name = name

	if k.c, err = newWriter(dir, name); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	if k.p = pp; k.p == nil {
		k.p = k.mergeChunk
	}

	// Initialize semaphore
	k.cs = make(semaphore, 1)
	// TODO: Decide if we want to offer the ability to pass a context here.
	// It might be nice to ensure history instances are properly shut down
	k.ctx, k.cancelFn = context.WithCancel(context.Background())
	go k.watch()
	// Associate returning pointer to created Kiroku
	kp = &k
	return
}

// Kiroku represents historical DB entries
type Kiroku struct {
	mux sync.RWMutex

	out *scribe.Scribe
	// Current chunk
	c *Writer

	// Last meta
	m Meta

	// Directory to store chunks
	dir string
	// Name of service
	name string

	// Writer semaphore
	cs semaphore

	// Post processing func
	p Processor

	// Context of history
	ctx context.Context
	// Context cancel func
	cancelFn func()
}

// Transaction will engage a new history transaction
func (k *Kiroku) Transaction(fn func(*Writer) error) (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()

	now := time.Now()
	unix := now.UnixNano()
	name := fmt.Sprintf("%s.chunk.%d", k.name, unix)

	var c *Writer
	if c, err = newWriter(k.dir, name); err != nil {
		return
	}

	c.init(&k.m, unix)

	if err = fn(c); err != nil {
		if deleteErr := k.deleteChunk(c); deleteErr != nil {
			k.out.Errorf("error deleting chunk <%s>: %v", name, err)
		}

		return
	}

	newMeta := *c.m
	if err = c.close(); err != nil {
		err = fmt.Errorf("error closing chunk: %v", err)
		return
	}

	k.cs.send()
	k.m = newMeta
	return
}

// Close will close the selected instance of Kiroku
func (k *Kiroku) Close() (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(k.c.close())
	return errs.Err()
}

func (k *Kiroku) initMeta() (err error) {
	var (
		filename string
		ok       bool
	)

	if filename, ok, err = k.getLast(); err != nil {
		return
	}

	if !ok {
		k.m = *k.c.m
		return
	}

	var f *os.File
	if f, err = os.Open(filename); err != nil {
		return
	}
	defer f.Close()

	var r *Reader
	if r, err = NewReader(f); err != nil {
		return
	}

	k.m = r.Meta()
	return
}

func (k *Kiroku) getTruncatedName(filename string) (name string) {
	return strings.Replace(filename, k.dir+"/", "", 1)
}

func (k *Kiroku) getNext() (filename string, ok bool, err error) {
	fn := walkFn(func(iteratingName string, info os.FileInfo) (err error) {
		if !k.isWriterMatch(iteratingName, info) {
			return
		}

		// We found a match, set <filename> to the iterating name and set <ok> to true
		filename = iteratingName
		ok = true
		return errBreak
	})

	if err = filepath.Walk(k.dir, fn); err == errBreak {
		err = nil
	}

	return
}

func (k *Kiroku) getLast() (filename string, ok bool, err error) {
	fn := walkFn(func(iteratingName string, info os.FileInfo) (err error) {
		isMatch := k.isWriterMatch(iteratingName, info)
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

	if err = filepath.Walk(k.dir, fn); err == errBreak {
		err = nil
	}

	return
}

func (k *Kiroku) isWriterMatch(filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := k.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, k.name+".chunk") {
		// We do not have a service match, return
		return
	}

	return true
}

func (k *Kiroku) watch() {
	var (
		filename string

		ok  bool
		err error
	)

	for !k.isClosed() {
		if filename, ok, err = k.getNext(); err != nil {
			// TODO: Get teams input on if this value should be configurable
			k.out.Errorf("error getting next chunk filename: <%v>, sleeping for a minute and trying again", err)
			time.Sleep(time.Minute)
			continue
		}

		if !ok {
			k.waitForNext()
			continue
		}

		if err = k.processWriter(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			k.out.Errorf("error encountered during processing chunk: <%v>, sleeping for a minute and trying again", err)
			time.Sleep(time.Minute)
		}
	}
}

func (k *Kiroku) waitForNext() {
	select {
	case <-k.cs:
	case <-k.ctx.Done():
	}
}

func (k *Kiroku) processWriter(filename string) (err error) {
	var (
		m *Meta
		f *os.File
		r io.ReadSeeker
	)

	if m, f, err = newProcessorPair(filename); err != nil {
		return
	}
	defer f.Close()

	if r, err = newReader(f); err != nil {
		return
	}

	if err = k.p(m, r); err != nil {
		err = fmt.Errorf("error encountered during processing action: <%v>", err)
		return
	}

	if err = os.Remove(filename); err != nil {
		err = fmt.Errorf("error encountered while removing file: <%v>", err)
		return
	}

	return
}

func (k *Kiroku) mergeChunk(m *Meta, r io.ReadSeeker) (err error) {
	if err = k.c.merge(m, r); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	return
}

func (k *Kiroku) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	errs.Push(w.close())
	errs.Push(os.Remove(w.filename))
	return errs.Err()
}

func (k *Kiroku) isClosed() bool {
	select {
	case <-k.ctx.Done():
		return true
	default:
		return false
	}
}
