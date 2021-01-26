package history

import (
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/gdbu/atoms"
)

var metaSize = int64(unsafe.Sizeof(Meta{}))

func newMetaFromBytes(bs []byte) *Meta {
	// Associate meta with memory mapped bytes
	return (*Meta)(unsafe.Pointer(&bs[0]))
}

func newMetaFromFile(f *os.File) (m *Meta, err error) {
	if _, err = f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error encountered while seeking to beginning of file: %v", err)
		return
	}

	metaBS := make([]byte, metaSize)
	if _, err = io.ReadAtLeast(f, metaBS, int(metaSize)); err != nil {
		err = fmt.Errorf("error reading meta bytes: %v", err)
		return
	}

	m = newMetaFromBytes(metaBS)
	return
}

// Meta represents the historical meta data
type Meta struct {
	// CurrentIndex would be the current index count
	CurrentIndex atoms.Int64
	// RowCount is the number of rows contained within the History
	RowCount atoms.Int64
	// LastSnapshot is the timestamp of the last snapshot as Unix Nano
	LastSnapshot atoms.Int64
	// CreatedAt is a UnixNano timestamp of when the last chunk was created
	CreatedAt atoms.Int64
}

func (m *Meta) merge(in *Meta) {
	if in == nil {
		return
	}

	m.CurrentIndex.Store(in.CurrentIndex.Load())
	m.RowCount.Store(in.RowCount.Load())
	m.LastSnapshot.Store(in.LastSnapshot.Load())
	m.CreatedAt.Store(in.CreatedAt.Load())
}
