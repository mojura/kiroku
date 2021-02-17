package kiroku

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
	// Set filename as a combination of the provided directory, name, and a .moj extension
	w.filename = path.Join(dir, name+".moj")
	// Open target file
	// Note: This will create the file if it does not exist
	if w.f, err = os.OpenFile(w.filename, os.O_CREATE|os.O_RDWR, 0744); err != nil {
		err = fmt.Errorf("error opening file \"%s\": %v", w.filename, err)
		return
	}
	// Whenever function ends, close the Writer if an error was encountered
	defer w.closeIfError(err)

	// Associate meta with memory map of meta bytes within the Chunk
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

// GetIndex will get the current index value
func (w *Writer) GetIndex() (index int64) {
	w.mux.RLock()
	defer w.mux.RUnlock()

	// Return current index
	return w.m.CurrentIndex
}

// NextIndex will get the current index value then increment the internal value
func (w *Writer) NextIndex() (index int64) {
	w.mux.Lock()
	defer w.mux.Unlock()

	// Get current index
	index = w.m.CurrentIndex
	// Increment current index
	w.m.CurrentIndex++
	return
}

// SetIndex will set the index value
// Note: This can be used to manually set an index to a desired value
func (w *Writer) SetIndex(index int64) {
	w.mux.Lock()
	defer w.mux.Unlock()

	// Set current index as the provided index
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
	w.m.BlockCount++
	// Set total block size
	w.m.TotalBlockSize = w.w.Written()
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

	// Get Meta of Reader
	m := r.Meta()

	// Check to see if the Meta has already been consumed
	if m.CreatedAt <= w.m.CreatedAt {
		// Meta is stale, return
		return
	}

	// Copy remaining bytes to chunk
	if _, err = r.CopyBlocks(w.f); err != nil {
		err = fmt.Errorf("error encountered while copying source blocks: %v", err)
		return
	}

	// Merge new meta with existing meta
	w.m.merge(&m)
	return
}

func (w *Writer) setSize() (err error) {
	var fi os.FileInfo
	// Get file information
	if fi, err = w.f.Stat(); err != nil {
		err = fmt.Errorf("error getting file information: %v", err)
		return
	}

	// Check file size
	if fi.Size() >= metaSize {
		// File is at least as big as Meta size, nothing else is needed!
		return
	}

	// Extend file to be big enough for the Meta bytes
	if err = w.f.Truncate(metaSize); err != nil {
		err = fmt.Errorf("error setting file size to %d: %v", metaSize, err)
		return
	}

	return
}

func (w *Writer) mapMeta() (err error) {
	// Ensure underlying file is big enough for Meta bytes
	if err = w.setSize(); err != nil {
		err = fmt.Errorf("error setting size: %v", err)
		return
	}

	// Map bytes equal to the size of the Meta
	if w.mm, err = mmap.MapRegion(w.f, int(metaSize), os.O_RDWR, 0, 0); err != nil {
		err = fmt.Errorf("error initializing MMAP: %v", err)
		return
	}

	// Associate Meta with memory mapped bytes
	w.m = (*Meta)(unsafe.Pointer(&w.mm[0]))
	return
}

func (w *Writer) unmapMeta() (err error) {
	// Ensure MMAP is set
	if w.mm == nil {
		// MMAP not set, return
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
	errs.Push(w.w.Close())
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
