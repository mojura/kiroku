package kiroku

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/hatchify/errors"
)

func newMappedMeta(opts Options) (mm *mappedMeta, err error) {
	var m mappedMeta
	filename := opts.FullName() + ".kir"
	filepath := path.Join(opts.Dir, filename)
	if m.f, err = os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0744); err != nil {
		return
	}

	if err = m.mapMeta(); err != nil {
		return
	}

	mm = &m
	return
}

type mappedMeta struct {
	mux sync.RWMutex

	m *Meta

	f  *os.File
	mm mmap.MMap

	closed bool
}

func (m *mappedMeta) Get() (meta Meta) {
	m.mux.RLock()
	defer m.mux.RUnlock()
	if m.closed {
		return
	}

	meta = *m.m
	return
}

func (m *mappedMeta) Set(meta Meta) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return
	}

	*m.m = meta
	return
}

func (m *mappedMeta) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	if err = m.unmapMeta(); err != nil {
		return
	}

	return m.f.Close()
}

func (m *mappedMeta) mapMeta() (err error) {
	// Ensure underlying file is big enough for Meta bytes
	if err = m.setSize(); err != nil {
		err = fmt.Errorf("error setting file size: %v", err)
		return
	}

	// Map bytes equal to the size of the Meta
	if m.mm, err = mmap.MapRegion(m.f, int(metaSize), os.O_RDWR, 0, 0); err != nil {
		err = fmt.Errorf("error initializing MMAP: %v", err)
		return
	}

	// Associate Meta with memory mapped bytes
	m.m = newMetaFromBytes(m.mm)
	return
}

func (m *mappedMeta) unmapMeta() (err error) {
	// Ensure MMAP is set
	if m.mm == nil {
		// MMAP not set, return
		return
	}

	// Unmap MMAP file
	err = m.mm.Unmap()
	// Unset mmap value
	m.mm = nil
	// Unset meta value
	m.m = nil
	return
}

func (m *mappedMeta) setSize() (err error) {
	var fi os.FileInfo
	// Get file information
	if fi, err = m.f.Stat(); err != nil {
		err = fmt.Errorf("error getting file information: %v", err)
		return
	}

	// Check file size
	if fi.Size() >= metaSize {
		// File is at least as big as Meta size, nothing else is needed!
		return
	}

	// Extend file to be big enough for the Meta bytes
	if err = m.f.Truncate(metaSize); err != nil {
		err = fmt.Errorf("error setting file size to %d: %v", metaSize, err)
		return
	}

	return
}
