package history

import (
	"fmt"
	"io"
	"os"
	"unsafe"
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
	CurrentIndex int64
	// RowCount is the number of rows contained within the History
	RowCount int64
	// LastSnapshot is the timestamp of the last snapshot as Unix Nano
	LastSnapshot int64
	// CreatedAt is a UnixNano timestamp of when the last chunk was created
	CreatedAt int64
}

func (m *Meta) merge(in *Meta) {
	if in == nil {
		return
	}

	*m = *in
}
