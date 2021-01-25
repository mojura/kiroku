package history

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

// newChunk will initialize a new Chunk instance
func newChunk(dir, name string) (cc *Chunk, err error) {
	var c Chunk
	c.filename = path.Join(dir, name+".moj")
	if c.f, err = os.OpenFile(c.filename, os.O_CREATE|os.O_RDWR, 0744); err != nil {
		err = fmt.Errorf("error opening file \"%s\": %v", c.filename, err)
		return
	}
	defer c.closeIfError(err)

	if err = c.mapMeta(); err != nil {
		return
	}

	// Associate returning pointer to created Chunk
	cc = &c
	return
}

// Chunk represents historical DB entries
type Chunk struct {
	mux sync.RWMutex

	// Target file
	f *os.File
	// Enkodo writer
	w *enkodo.Writer

	// Memory map for Meta information
	mm mmap.MMap
	// Meta information
	m *Meta
	// Location of file
	filename string
}

// SetIndex will set the index value
func (c *Chunk) SetIndex(index int64) {
	c.m.CurrentIndex.Store(index)
}

// AddRow will add a row
func (c *Chunk) AddRow(t Type, data []byte) (err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	var b Block
	b.Type = t
	b.Data = data

	// Encode block to writer
	if err = c.w.Encode(&b); err != nil {
		return
	}

	// Increment row count
	c.m.RowCount.Add(1)
	return
}

func (c *Chunk) init(m *Meta) {
	// Populate meta info
	c.m.merge(m)
	// Set chunk createdAt time
	c.m.CreatedAt.Store(time.Now().UnixNano())
}

func (c *Chunk) merge(m *Meta, r io.Reader) (err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if m.CreatedAt.Load() <= c.m.CreatedAt.Load() {
		return
	}

	// Copy remaining bytes to chunk
	if _, err = io.Copy(c.f, r); err != nil {
		return
	}

	// Merge new meta with existing meta
	c.m.merge(m)
	return
}

func (c *Chunk) setSize() (err error) {
	var fi os.FileInfo
	if fi, err = c.f.Stat(); err != nil {
		err = fmt.Errorf("error getting file information: %v", err)
		return
	}

	if fi.Size() < metaSize {
		return
	}

	if err = c.f.Truncate(metaSize); err != nil {
		err = fmt.Errorf("error setting file size to %d: %v", metaSize, err)
		return
	}

	return
}

func (c *Chunk) mapMeta() (err error) {
	if err = c.setSize(); err != nil {
		err = fmt.Errorf("error setting size: %v", err)
		return
	}

	// Map bytes equal to the size of the meta
	if c.mm, err = mmap.MapRegion(c.f, int(metaSize), os.O_RDWR, 0, 0); err != nil {
		err = fmt.Errorf("error initializing MMAP: %v", err)
		return
	}

	// Associate meta with memory mapped bytes
	c.m = (*Meta)(unsafe.Pointer(&c.mm[0]))
	return
}

func (c *Chunk) unmapMeta() (err error) {
	if c.mm == nil {
		return
	}

	// Unmap MMAP file
	err = c.mm.Unmap()
	// Unset mmap value
	c.mm = nil
	// Unset meta value
	c.m = nil
	return
}

func (c *Chunk) close() (err error) {
	var errs errors.ErrorList
	errs.Push(c.unmapMeta())
	errs.Push(c.f.Close())
	return errs.Err()
}

// Close will close the selected instance of Chunk
func (c *Chunk) closeIfError(err error) {
	if err == nil {
		return
	}

	c.close()
}
