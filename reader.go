package history

import (
	"io"
	"os"

	"github.com/mojura/enkodo"
)

// NewReader will initialize a new chunk reader
func NewReader(f *os.File) (rp *Reader, err error) {
	var r Reader
	if r.m, err = newMetaFromFile(f); err != nil {
		return
	}

	r.f = f
	rp = &r
	return
}

// Reader will parse and read a chunk
type Reader struct {
	m *Meta
	f *os.File
}

// Meta will return the meta information for the chunk
func (r *Reader) Meta() Meta {
	return *r.m
}

// ForEach will iterate through all the blocks within the reader
func (r *Reader) ForEach(fn func(*Block) error) (err error) {
	if _, err = r.f.Seek(metaSize, 0); err != nil {
		return
	}

	rdr := enkodo.NewReader(r.f)
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
	if _, err = r.f.Seek(0, 0); err != nil {
		return
	}

	return io.Copy(destination, r.f)
}

// CopyBlocks will copy the blocks only (no meta)
func (r *Reader) CopyBlocks(destination io.Writer) (n int64, err error) {
	if _, err = r.f.Seek(metaSize, 0); err != nil {
		return
	}

	return io.Copy(destination, r.f)
}
