package history

import (
	"fmt"
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
	m Meta
	r io.ReadSeeker
}

// Meta will return the meta information for the chunk
func (r *Reader) Meta() Meta {
	// Return a copy of the underlying Meta
	return r.m
}

// ForEach will iterate through all the blocks within the reader
func (r *Reader) ForEach(fn func(*Block) error) (err error) {
	// Seek to the first block byte
	if _, err = r.r.Seek(metaSize, 0); err != nil {
		err = fmt.Errorf("error seeking to first block byte: %v", err)
		return
	}

	// Initialize a new Enkodo reader
	rdr := enkodo.NewReader(r.r)

	// Iterate until break
	for {
		var b Block
		// Decode next block
		if err = rdr.Decode(&b); err != nil {
			// Error encountered while decoding, break out of the loop
			break
		}

		// Call provided function
		if err = fn(&b); err != nil {
			// Function returned an error, return
			return
		}
	}

	if err == io.EOF {
		// Error was io.EOF, set to nil
		err = nil
	}

	return
}

// Copy will copy the entire chunk (meta + blocks)
func (r *Reader) Copy(destination io.Writer) (n int64, err error) {
	// Seek to the beginning of the file
	if _, err = r.r.Seek(0, 0); err != nil {
		err = fmt.Errorf("error seeking to first meta byte: %v", err)
		return
	}

	// Copy entire chunk to destination writer
	return io.Copy(destination, r.r)
}

// CopyBlocks will copy the blocks only (no meta)
func (r *Reader) CopyBlocks(destination io.Writer) (n int64, err error) {
	// Seek to the beginning of the blocks
	if _, err = r.r.Seek(metaSize, 0); err != nil {
		err = fmt.Errorf("error seeking to first block byte: %v", err)
		return
	}

	// Copy chunk blocks to destination writer
	return io.Copy(destination, r.r)
}
