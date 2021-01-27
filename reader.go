package history

import (
	"io"

	"github.com/mojura/enkodo"
)

// NewReader will initialize a new chunk reader
func NewReader(rs io.ReadSeeker) (rp *Reader, err error) {
	var r Reader
	if r.m, err = newMetaFromReader(rs); err != nil {
		return
	}

	r.r = rs
	rp = &r
	return
}

// Reader will parse and read a history chunk
type Reader struct {
	m *Meta
	r io.ReadSeeker
}

// Meta will return the meta information for the chunk
func (r *Reader) Meta() Meta {
	return *r.m
}

// ForEach will iterate through all the blocks within the reader
func (r *Reader) ForEach(fn func(*Block) error) (err error) {
	if _, err = r.r.Seek(metaSize, 0); err != nil {
		return
	}

	rdr := enkodo.NewReader(r.r)
	for {
		var b Block
		if err = rdr.Decode(&b); err != nil {
			break
		}

		if err = fn(&b); err != nil {
			return
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}

// Copy will copy the entire chunk (meta + blocks)
func (r *Reader) Copy(destination io.Writer) (n int64, err error) {
	if _, err = r.r.Seek(0, 0); err != nil {
		return
	}

	return io.Copy(destination, r.r)
}

// CopyBlocks will copy the blocks only (no meta)
func (r *Reader) CopyBlocks(destination io.Writer) (n int64, err error) {
	if _, err = r.r.Seek(metaSize, 0); err != nil {
		return
	}

	return io.Copy(destination, r.r)
}
