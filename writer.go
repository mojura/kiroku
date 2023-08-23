package kiroku

import (
	"os"
	"path"
	"sync"

	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

var ErrEmptyBlock = errors.New("invalid block, cannot be empty")

func newWriter(dir string, filename Filename) (wp *Writer, err error) {
	var w Writer
	w.filename = filename
	// Set filename as a combination of the provided directory, name, and a .kir extension
	w.filepath = path.Join(dir, w.filename.String())
	// Open target file
	// Note: This will create the file if it does not exist
	if w.f, err = createAppendFile(w.filepath); err != nil {
		return
	}

	// Initialize enkodo writer
	w.w = enkodo.NewWriter(w.f)
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
	filename Filename
	filepath string

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
