package history

import (
	"fmt"
	"os"
	"path"
	"sync"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

// newWriter will initialize a new Writer instance
func newWriter(dir, name string) (wp *Writer, err error) {
	var w Writer
	w.filename = path.Join(dir, name+".moj")
	if w.f, err = os.OpenFile(w.filename, os.O_CREATE|os.O_RDWR, 0744); err != nil {
		err = fmt.Errorf("error opening file \"%s\": %v", w.filename, err)
		return
	}
	defer w.closeIfError(err)

	if err = w.mapMeta(); err != nil {
		return
	}

	// Move past meta
	if _, err = w.f.Seek(metaSize, 0); err != nil {
		return
	}

	// Initialize enkodo writer
	w.w = enkodo.NewWriter(w.f)
	// Associate returning pointer to created Writer
	wp = &w
	return
}

// Writer will write a history chunk
type Writer struct {
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

// GetIndex will get the index value
func (w *Writer) GetIndex() (index int64) {
	return w.m.CurrentIndex
}

// SetIndex will set the index value
func (w *Writer) SetIndex(index int64) {
	w.m.CurrentIndex = index
}

// AddRow will add a row
func (w *Writer) AddRow(t Type, data []byte) (err error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	var b Block
	b.Type = t
	b.Data = data

	// Encode block to writer
	if err = w.w.Encode(&b); err != nil {
		return
	}

	// Increment row count
	w.m.RowCount++
	return
}

func (w *Writer) init(m *Meta, createdAt int64) {
	// Populate meta info
	w.m.merge(m)
	// Set chunk createdAt time
	w.m.CreatedAt = createdAt
}

func (w *Writer) merge(r *Reader) (err error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	m := r.Meta()
	if m.CreatedAt <= w.m.CreatedAt {
		return
	}

	// Copy remaining bytes to chunk
	if _, err = r.CopyBlocks(w.f); err != nil {
		return
	}

	// Merge new meta with existing meta
	w.m.merge(&m)
	return
}

func (w *Writer) setSize() (err error) {
	var fi os.FileInfo
	if fi, err = w.f.Stat(); err != nil {
		err = fmt.Errorf("error getting file information: %v", err)
		return
	}

	if fi.Size() >= metaSize {
		return
	}

	if err = w.f.Truncate(metaSize); err != nil {
		err = fmt.Errorf("error setting file size to %d: %v", metaSize, err)
		return
	}

	return
}

func (w *Writer) mapMeta() (err error) {
	if err = w.setSize(); err != nil {
		err = fmt.Errorf("error setting size: %v", err)
		return
	}

	// Map bytes equal to the size of the meta
	if w.mm, err = mmap.MapRegion(w.f, int(metaSize), os.O_RDWR, 0, 0); err != nil {
		err = fmt.Errorf("error initializing MMAP: %v", err)
		return
	}

	// Associate meta with memory mapped bytes
	w.m = (*Meta)(unsafe.Pointer(&w.mm[0]))
	return
}

func (w *Writer) unmapMeta() (err error) {
	if w.mm == nil {
		return
	}

	// Unmap MMAP file
	err = w.mm.Unmap()
	// Unset mmap value
	w.mm = nil
	// Unset meta value
	w.m = nil
	return
}

func (w *Writer) close() (err error) {
	var errs errors.ErrorList
	errs.Push(w.unmapMeta())
	errs.Push(w.f.Close())
	return errs.Err()
}

// Close will close the selected instance of Writer
func (w *Writer) closeIfError(err error) {
	if err == nil {
		return
	}

	w.close()
}
