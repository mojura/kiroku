package kiroku

import (
	"fmt"
	"io"
	"os"

	"github.com/mojura/enkodo"
)

// NewReader will initialize a new chunk reader
func NewReader(f File) (rp *Reader) {
	var r Reader
	r.r = f
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
	// Initialize a new Reader utilizing the recently opened file
	r := NewReader(f)
	// Call provided Processor and return whatever error it produces
	return p(r)
}

// Reader will parse and read a history chunk
type Reader struct {
	r io.ReadSeeker
}

// ForEach will iterate through all the blocks within the reader
func (r *Reader) ForEach(seek int64, fn func(Block) error) (err error) {
	// Seek to the first block byte
	if _, err = r.r.Seek(0, 0); err != nil {
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
		if err = fn(b); err != nil {
			// Function returned an error, return
			return
		}
	}

	return r.handleError(err)
}

// Copy will copy the entire reader
func (r *Reader) Copy(destination io.Writer) (n int64, err error) {
	// Seek to the beginning of the file
	if _, err = r.r.Seek(0, 0); err != nil {
		err = fmt.Errorf("error seeking to first byte: %v", err)
		return
	}

	// Copy entire chunk to destination writer
	return io.Copy(destination, r.r)
}

// ReadSeeker will return the Reader's underlying ReadSeeker
func (r *Reader) ReadSeeker() io.ReadSeeker {
	return r.r
}

func (r *Reader) handleError(inbound error) (err error) {
	switch inbound {
	case nil:
		return nil
	case io.EOF:
		return nil

	default:
		return fmt.Errorf("error decoding block: %v", inbound)
	}
}
