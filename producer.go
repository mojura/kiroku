package kiroku

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

const errBreak = errors.Error("break")

// New will initialize a new Producer instance
// Note: Processor and Options are optional
func New(o Options, src Source) (kp *Producer, err error) {
	// Call NewWithContext with a background context
	return NewWithContext(context.Background(), o, src)
}

// NewWithContext will initialize a new Producer instance with a provided context.Context
// Note: Processor and Options are optional
func NewWithContext(ctx context.Context, o Options, src Source) (kp *Producer, err error) {
	if err = o.Validate(); err != nil {
		return
	}

	var p Producer
	// Set output prefix
	prefix := fmt.Sprintf("Producer (%v)", o.Name)
	// Initialize Producer output
	p.out = scribe.New(prefix)
	// Set options
	p.opts = o
	// Set directory as a cleaned version of the provided directory
	p.opts.Dir = filepath.Clean(p.opts.Dir)
	// Initialize cancel context with the provided context as the parent
	p.ctx, p.cancelFn = context.WithCancel(ctx)
	// Set exporter
	// Note: This field is optional and might be nil
	p.src = src
	// Set source state
	p.hasSource = !isNilSource(src)

	p.w = newWatcher(p.ctx, o, p.out, "chunk", p.exportAndRemove)
	p.b = newBatcher(p.opts.BatchDuration, p.Transaction)
	kp = &p
	return
}

// Producer represents historical DB entries
type Producer struct {
	mux sync.RWMutex

	// Context of history
	ctx context.Context
	// Context cancel func
	cancelFn func()

	// Output logger
	out *scribe.Scribe

	// Producer options
	opts Options

	src       Source
	hasSource bool

	w *watcher
	b *batcher
}

// Transaction will engage a new history transaction
func (p *Producer) Transaction(fn TransactionFn) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	// Check to see if Producer is closed
	if isClosed(p.ctx) {
		return errors.ErrIsClosed
	}

	txnFn := func(w *Writer) (err error) {
		txn := newTransaction(w)

		// Call provided function
		return fn(txn)
	}

	return p.transaction(TypeChunk, txnFn)
}

// Snapshot will engage a new history snapshot
func (p *Producer) Snapshot(fn func(*Snapshot) error) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	// Check to see if Producer is closed
	if isClosed(p.ctx) {
		return errors.ErrIsClosed
	}

	txnFn := func(w *Writer) (err error) {
		// Initialize snapshot
		ss := newSnapshot(w)
		// Call provided function
		return fn(ss)
	}

	return p.transaction(TypeSnapshot, txnFn)
}

// Batch will engage a new history batch transaction
func (p *Producer) Batch(fn BatchFn) (err error) {
	return p.b.Batch(fn)
}

// Batch will engage a new history batch transaction
func (p *Producer) BatchBlock(value []byte) (err error) {
	berr := p.Batch(func(txn *Transaction) {
		err = txn.Write(value)
	})

	switch {
	case err != nil:
		return err
	case berr != nil:
		return berr
	default:
		return nil
	}
}

// Close will close the selected instance of Producer
func (p *Producer) Close() (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	// Check to see if Producer is closed
	if isClosed(p.ctx) {
		return errors.ErrIsClosed
	}

	// Cancel the context
	p.cancelFn()

	// Wait for jobs to finish
	p.w.waitToComplete()

	var errs errors.ErrorList
	if !p.opts.AvoidExportOnClose {
		// Options do not request avoiding merge on close, process remaining merged chunks
		errs.Push(p.w.processAll("merged"))
	}

	return errs.Err()
}

func (p *Producer) rename(f Filename, t Type) (err error) {
	newName := f
	newName.filetype = t

	oldFilename := path.Join(p.opts.Dir, f.String())
	// Set filename as directory and name joined
	newFilename := path.Join(p.opts.Dir, newName.String())

	// Rename original filename as new filename
	if err = os.Rename(oldFilename, newFilename); err != nil {
		return
	}

	return
}

func (p *Producer) export(filename Filename) (err error) {
	if !p.hasSource {
		// Exporter not set, return
		return
	}

	var f *os.File
	filepath := path.Join(p.opts.Dir, filename.String())
	if f, err = os.Open(filepath); err != nil {
		err = fmt.Errorf("error opening <%s>: %v", filename, err)
		return
	}
	defer f.Close()

	if err = p.src.Export(context.Background(), filename.String(), f); err != nil {
		err = fmt.Errorf("error exporting <%s>: %v", filename.String(), err)
		return
	}

	if filename.filetype != TypeSnapshot {
		return
	}

	rdr := strings.NewReader(filename.String())
	snapshotName := getSnapshotName(p.opts.FullName())
	if err = p.src.Export(context.Background(), snapshotName, rdr); err != nil {
		err = fmt.Errorf("error setting latest snapshot: %v", err)
		return
	}

	return
}

func (p *Producer) exportAndRemove(f Filename) (err error) {
	// Export file
	if err = p.export(f); err != nil {
		return
	}

	filepath := path.Join(p.opts.Dir, f.String())
	return os.Remove(filepath)
}

func (p *Producer) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	// Close target chunk
	errs.Push(w.Close())
	// Remove target chunk
	errs.Push(os.Remove(w.filepath))
	return errs.Err()
}

func (p *Producer) transaction(t Type, fn func(*Writer) error) (err error) {

	// Get current timestamp
	now := time.Now()
	// Get Unix nano value from timestamp
	unix := now.UnixNano()
	// Set name of chunk with temporary prefix
	name := makeFilename(p.opts.FullName(), unix, TypeTemporary)

	var w *Writer
	// Initialize a new chunk Writer
	if w, err = newWriter(p.opts.Dir, name); err != nil {
		return
	}

	// Call provided function
	if err = fn(w); err != nil {
		// Error encountered, delete chunk!
		if deleteErr := p.deleteChunk(w); deleteErr != nil {
			// Error encountered while deleting chunk, leave error log to notify server manager
			p.out.Errorf("error deleting chunk <%s>: %v", name, deleteErr)
		}

		// Return error from provided function
		return
	}

	_ = w.Close()
	if w.blockCount == 0 {
		_ = os.Remove(w.filepath)
		return
	}

	// Rename to chunk with
	if err = p.rename(w.filename, t); err != nil {
		return
	}

	// Send signal to chunk watcher
	p.w.trigger()
	return
}
