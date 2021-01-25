package history

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

// New will initialize a new History instance
// Note: PostProcessor is optional
func New(dir, name string, pp PostProcessor) (hp *History, err error) {
	var h History
	h.out = scribe.New(fmt.Sprintf("Mojura history (%v)", name))
	h.dir = dir
	h.name = name

	if h.c, err = newChunk(dir, name); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	h.mc = make(chan *os.File, 12)
	h.pc = make(chan *processorPayload, 12)
	h.pp = pp
	// Associate returning pointer to created History
	hp = &h
	return
}

// History represents historical DB entries
type History struct {
	mux sync.RWMutex

	out *scribe.Scribe
	// Current chunk
	c *Chunk

	// Directory to store chunks
	dir string
	// Name of service
	name string

	// Merge channel
	mc chan *os.File
	// Process channel
	pc chan *processorPayload
	// Post processing func
	pp PostProcessor
}

func (h *History) deleteChunk(c *Chunk) (err error) {
	var errs errors.ErrorList
	errs.Push(c.close())
	errs.Push(os.Remove(c.filename))
	return errs.Err()
}

func (h *History) mergeChunk(f *os.File) (pp *processorPayload, err error) {
	if _, err = f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error encountered while seeking to beginning of file: %v", err)
		return
	}

	metaBS := make([]byte, metaSize)
	if _, err = io.ReadAtLeast(f, metaBS, int(metaSize)); err != nil {
		err = fmt.Errorf("error reading meta bytes: %v", err)
		return
	}

	var p processorPayload
	// Create meta from bytes
	p.m = newMetafromBytes(metaBS)
	// Associate chunk file as payload read seeker
	p.r = f

	if err = h.c.merge(p.m, f); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	pp = &p
	return
}

func (h *History) watchMerge() {
	for f := range h.mc {
		var (
			p   *processorPayload
			err error
		)

		if p, err = h.mergeChunk(f); err != nil {
			h.out.Errorf("error processing chunk: %v", err)
			// TODO: Figure out best course of action here
			continue
		}

		// Send processor payload down processor channel
		h.pc <- p
	}
}

func (h *History) processWatch() {
	for p := range h.pc {
		var err error
		if _, err = p.r.Seek(metaSize, 0); err != nil {
			// TODO: Figure out best plan of action here
			h.out.Errorf("error seeking process file: %v", err)
			continue
		}

		if err = h.pp(p.m, p.r); err != nil {
			h.out.Errorf("error running process file: %v", err)
			continue
		}

		if err = os.Remove(p.r.Name()); err != nil {
			h.out.Errorf("error removing processed chunk: %v", err)
			continue
		}
	}
}

// Transaction will engage a new history transaction
func (h *History) Transaction(fn func(*Chunk) error) (err error) {
	h.mux.Lock()
	defer h.mux.Unlock()

	now := time.Now()
	name := fmt.Sprintf("%s.chunk.%d", h.name, now.UnixNano())

	var c *Chunk
	if c, err = newChunk(h.dir, name); err != nil {
		return
	}

	if err = fn(c); err != nil {
		if deleteErr := h.deleteChunk(c); deleteErr != nil {
			h.out.Errorf("error deleting chunk <%s>: %v", name, err)
		}

		return
	}

	// Add chunk file to merge channel
	h.mc <- c.f
	return
}

// Close will close the selected instance of History
func (h *History) Close() (err error) {
	var errs errors.ErrorList
	errs.Push(h.c.close())
	return errs.Err()
}
