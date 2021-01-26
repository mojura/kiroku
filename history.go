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
func New(dir, name string, pp Processor) (hp *Kiroku, err error) {
	var h Kiroku
	prefix := fmt.Sprintf("Mojura history (%v)", name)
	h.out = scribe.New(prefix)
	h.dir = filepath.Clean(dir)
	h.name = name

	if h.c, err = newWriter(dir, name); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	if h.p = pp; h.p == nil {
		h.p = h.mergeChunk
	}

	// Initialize semaphore
	h.cs = make(semaphore, 1)
	// TODO: Decide if we want to offer the ability to pass a context here.
	// It might be nice to ensure history instances are properly shut down
	h.ctx, h.cancelFn = context.WithCancel(context.Background())
	go h.watch()
	// Associate returning pointer to created Kiroku
	hp = &h
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
func (h *Kiroku) Transaction(fn func(*Writer) error) (err error) {
	h.mux.Lock()
	defer h.mux.Unlock()

	now := time.Now()
	unix := now.UnixNano()
	name := fmt.Sprintf("%s.chunk.%d", h.name, unix)

	var c *Writer
	if c, err = newWriter(h.dir, name); err != nil {
		return
	}

	c.init(&h.m, unix)

	if err = fn(c); err != nil {
		if deleteErr := h.deleteChunk(c); deleteErr != nil {
			h.out.Errorf("error deleting chunk <%s>: %v", name, err)
		}

		return
	}

	newMeta := *c.m
	if err = c.close(); err != nil {
		err = fmt.Errorf("error closing chunk: %v", err)
		return
	}

	h.cs.send()
	h.m = newMeta
	return
}

// Close will close the selected instance of Kiroku
func (h *Kiroku) Close() (err error) {
	h.mux.Lock()
	defer h.mux.Unlock()
	if h.isClosed() {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(h.c.close())
	return errs.Err()
}

func (h *Kiroku) getTruncatedName(filename string) (name string) {
	return strings.Replace(filename, h.dir+"/", "", 1)
}

func (h *Kiroku) getNext() (filename string, ok bool, err error) {
	fn := walkFn(func(iteratingName string, info os.FileInfo) (err error) {
		if !h.isWriterMatch(iteratingName, info) {
			return
		}

		// We found a match, set <filename> to the iterating name and set <ok> to true
		filename = iteratingName
		ok = true
		return errBreak
	})

	if err = filepath.Walk(h.dir, fn); err == errBreak {
		err = nil
	}

	return
}

func (h *Kiroku) isWriterMatch(filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := h.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, h.name+".chunk") {
		// We do not have a service match, return
		return
	}

	return true
}

func (h *Kiroku) watch() {
	var (
		filename string

		ok  bool
		err error
	)

	for !h.isClosed() {
		if filename, ok, err = h.getNext(); err != nil {
			// TODO: Get teams input on if this value should be configurable
			h.out.Errorf("error getting next chunk filename: <%v>, sleeping for a minute and trying again", err)
			time.Sleep(time.Minute)
			continue
		}

		if !ok {
			h.waitForNext()
			continue
		}

		if err = h.processWriter(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			h.out.Errorf("error encountered during processing chunk: <%v>, sleeping for a minute and trying again", err)
			time.Sleep(time.Minute)
		}
	}
}

func (h *Kiroku) waitForNext() {
	select {
	case <-h.cs:
	case <-h.ctx.Done():
	}
}

func (h *Kiroku) processWriter(filename string) (err error) {
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

	if err = h.p(m, r); err != nil {
		err = fmt.Errorf("error encountered during processing action: <%v>", err)
		return
	}

	if err = os.Remove(filename); err != nil {
		err = fmt.Errorf("error encountered while removing file: <%v>", err)
		return
	}

	return
}

func (h *Kiroku) mergeChunk(m *Meta, r io.ReadSeeker) (err error) {
	if err = h.c.merge(m, r); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	return
}

func (h *Kiroku) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	errs.Push(w.close())
	errs.Push(os.Remove(w.filename))
	return errs.Err()
}

func (h *Kiroku) isClosed() bool {
	select {
	case <-h.ctx.Done():
		return true
	default:
		return false
	}
}
