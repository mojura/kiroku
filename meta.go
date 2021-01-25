package history

import (
	"unsafe"

	"github.com/gdbu/atoms"
)

var metaSize = int64(unsafe.Sizeof(Meta{}))

func newMetafromBytes(bs []byte) *Meta {
	// Associate meta with memory mapped bytes
	return (*Meta)(unsafe.Pointer(&bs[0]))
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
