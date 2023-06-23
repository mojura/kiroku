package kiroku

import (
	"fmt"
	"io"
	"os"

	"github.com/mojura/enkodo"
)

// NewReader will initialize a new chunk reader
func NewReader(f File) (rp *Reader, err error) {
	var r Reader
	if r.m, err = newMetaFromReader(f); err != nil {
		return
	}

	var end int64
	if end, err = r.getEnd(f); err != nil {
		return
	}

	r.r = io.NewSectionReader(f, 0, end)
	rp = &r
	return
}

// Read will read a filename and provide a temporary reader
func Read(filename string, p Processor) (err error) {
	var f *os.File
	// Open file with provided filename
	if f, err = os.Open(filename); err != nil {
		// Error opening file, return
		return
	}
	// Close file whenever the function ends
	defer f.Close()

	var r *Reader
	// Initialize a new Reader utilizing the recently opened file
	if r, err = NewReader(f); err != nil {
		return
	}

	// Call provided Processor and return whatever error it produces
	return p(r)
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
func (r *Reader) ForEach(seek int64, fn func(*Block) error) (err error) {
	seek += metaSize

	// Seek to the first block byte
	if _, err = r.r.Seek(seek, 0); err != nil {
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

	switch err {
	case nil:
		return nil
	case io.EOF:
		return nil

	default:
		return
	}
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

// ReadSeeker will return the Reader's underlying ReadSeeker
func (r *Reader) ReadSeeker() io.ReadSeeker {
	return r.r
}

func (r *Reader) getEnd(f File) (end int64, err error) {
	if end, err = f.Seek(0, io.SeekEnd); err != nil {
		return
	}

	if end > r.m.TotalBlockSize+metaSize {
		end = r.m.TotalBlockSize + metaSize
	}

	return
}
