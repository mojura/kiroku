package kiroku

import (
	"fmt"
	"io"
	"unsafe"
)

var metaSize = int64(unsafe.Sizeof(Meta{}))

func newMetaFromBytes(bs []byte) *Meta {
	// Associate meta with provided bytes
	return (*Meta)(unsafe.Pointer(&bs[0]))
}

func newMetaFromReader(r io.ReadSeeker) (m Meta, err error) {
	// Seek to the beginning of the files
	if _, err = r.Seek(0, 0); err != nil {
		err = fmt.Errorf("error encountered while seeking to beginning of file: %v", err)
		return
	}

	// Initialize buffer for Meta
	metaBS := make([]byte, metaSize)

	// Read Meta bytes to buffer
	if _, err = io.ReadAtLeast(r, metaBS, int(metaSize)); err != nil {
		err = fmt.Errorf("error reading meta bytes: %v", err)
		return
	}

	// Associate bytes as a pointer to Meta
	mp := newMetaFromBytes(metaBS)

	// Set Meta as a de-referenced Meta pointer
	// Note: This avoids the possibility of any GC weirdness due to
	// associating unsafely against a byteslice.
	m = *mp
	return
}

// Meta represents the historical meta data
type Meta struct {
	// CurrentIndex would be the current index count
	CurrentIndex uint64

	// BlockCount is the number of blocks contained within the Chunk
	BlockCount int64
	// TotalBlockSize is the total block size (in bytes)
	TotalBlockSize int64

	// LastSnapshotAt is the timestamp of the last snapshot as Unix Nano
	LastSnapshotAt int64
	// CreatedAt is a UnixNano timestamp of when the last chunk was created
	CreatedAt int64
}

func (m *Meta) merge(in *Meta) {
	// Check to see if inbound Meta exists
	if in == nil {
		// Inbound Meta does not exist, bail out
		return
	}

	mm := *in
	// Set total block size as a combination of the merging meta and the parent
	mm.TotalBlockSize += m.TotalBlockSize

	// Set the underlying Meta as the dereferenced value of the inbound Meta
	*m = mm
}
