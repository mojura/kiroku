package kiroku

import (
	"io"
	"os"
	"path"
	"sync"

	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

var ErrEmptyBlock = errors.New("invalid block, cannot be empty")

// NewWriter will initialize a new Writer instance
func NewWriter(dir, name string) (wp *Writer, err error) {
	// Create a new writer
	if wp, err = newWriter(dir, name); err != nil {
		return
	}

	return
}

func newWriter(dir, name string) (wp *Writer, err error) {
	var f *os.File
	// Set filename as a combination of the provided directory, name, and a .kir extension
	filename := path.Join(dir, name+".kir")
	// Open target file
	// Note: This will create the file if it does not exist
	if f, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0744); err != nil {
		return
	}

	return NewWriterWithFile(f)
}

// NewWriterWithFile will initialize a new Writer instance
func NewWriterWithFile(f *os.File) (wp *Writer, err error) {
	var w Writer
	w.f = f
	w.filename = f.Name()
	// Whenever function ends, close the Writer if an error was encountered
	defer func() { w.closeIfError(err) }()

	// Move to the end of the file
	if _, err = w.f.Seek(0, io.SeekEnd); err != nil {
		return
	}

	// Initialize enkodo writer
	w.w = enkodo.NewWriter(w.f)
	// Associate returning pointer to created Writer
	wp = &w
	return
}

// Writer will write a history chunk
type Writer struct {
	mux sync.RWMutex

	// Target file
	f *os.File
	// Enkodo writer
	w *enkodo.Writer

	// Location of file
	filename string

	blockCount int

	closed bool
}

// AddBlock will add a row
func (w *Writer) Write(value Block) (err error) {
	if len(value) == 0 {
		return ErrEmptyBlock
	}

	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return errors.ErrIsClosed
	}

	// Encode block to writer
	if err = w.w.Encode(value); err != nil {
		return
	}

	w.blockCount++
	return
}

// Filename will return the underlying filename of the Writer
func (w *Writer) Filename() string {
	return w.filename
}

// Close will close a writer
func (w *Writer) Close() (err error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return errors.ErrIsClosed
	}

	w.closed = true

	var errs errors.ErrorList
	if w.w != nil {
		errs.Push(w.w.Close())
	}

	if w.f != nil {
		errs.Push(w.f.Close())
	}

	return errs.Err()
}

// Close will close the selected instance of Writer
func (w *Writer) closeIfError(err error) {
	if err == nil {
		return
	}

	w.Close()
}
