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

// New will initialize a new Producer instance
// Note: Processor and Options are optional
func NewProducer(o Options, src Source) (kp *Producer, err error) {
	// Call NewWithContext with a background context
	return NewProducerWithContext(context.Background(), o, src)
}

// NewWithContext will initialize a new Producer instance with a provided context.Context
// Note: Processor and Options are optional
func NewProducerWithContext(ctx context.Context, o Options, src Source) (pp *Producer, err error) {
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
	// Initialize semaphores
	p.ms = make(semaphore, 1)
	p.es = make(semaphore, 1)

	// Initialize primary Chunk
	if p.c, err = NewWriter(p.opts.Dir, p.opts.FullName()); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	p.b = newBatcher(00, p.Transaction)

	// Increment jobs waiter
	p.jobs.Add(2)
	// Initialize watch job
	go p.watch("chunk", p.ms, p.onChunk)
	go p.watch("merged", p.es, p.onMerge)
	// Associate returning pointer to created Producer
	pp = &p
	return
}

// Producer represents historical DB entries
type Producer struct {
	mux sync.RWMutex

	// Output logger
	out *scribe.Scribe

	// Producer options
	opts Options

	src       Source
	hasSource bool

	// Goroutine job waiter
	jobs sync.WaitGroup

	// Merging semaphore
	ms semaphore
	// Exporting semaphore
	es semaphore

	// Primary chunk
	c *Writer

	b *batcher

	// Context of history
	ctx context.Context
	// Context cancel func
	cancelFn func()
}

// Transaction will engage a new history transaction
func (p *Producer) Transaction(fn TransactionFn) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	// Check to see if Producer is closed
	if p.isClosed() {
		return errors.ErrIsClosed
	}

	txnFn := func(w *Writer) (err error) {
		txn := newTransaction(w)

		// Call provided function
		return fn(txn)
	}

	return p.transaction(txnFn)
}

// Batch will engage a new history batch transaction
func (p *Producer) Batch(fn BatchFn) (err error) {
	return p.b.Batch(fn)
}

// Batch will engage a new history batch transaction
func (p *Producer) BatchBlock(typ Type, key []byte, value []byte) (err error) {
	berr := p.b.Batch(func(txn *Transaction) {
		err = txn.AddBlock(typ, key, value)
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
	if p.isClosed() {
		return errors.ErrIsClosed
	}

	// Cancel the context
	p.cancelFn()

	// Wait for jobs to finish
	p.jobs.Wait()

	var errs errors.ErrorList
	if !p.opts.AvoidMergeOnClose {
		// Options do not request avoiding merge on close, merge remaining chunks
		errs.Push(p.handleRemaining("chunk", p.onChunk))
	}

	if !p.opts.AvoidExportOnClose {
		// Options do not request avoiding merge on close, process remaining merged chunks
		errs.Push(p.handleRemaining("merged", p.onMerge))
	}

	// Close primary chunk
	errs.Push(p.c.Close())
	return errs.Err()
}

func (p *Producer) getTruncatedName(filename string) (name string) {
	// Truncate name by removing directory
	// TODO: There might have been a reason it was setup this way instead of using
	// path.Base, unfortunately I forgot to leave a comment as to why I did so. This
	// is a note to do some discovery around this. The outcome should be one of two:
	//	1. Comment as to why this approach was used
	//	2. Use path.Base
	return strings.Replace(filename, p.opts.Dir+"/", "", 1)
}

func (p *Producer) getNext(targetPrefix string) (filename string, ok bool, err error) {
	fn := func(iteratingName string, info os.FileInfo) (err error) {
		// Check to see if current file is a match for the current name and prefix
		if !p.isWriterMatch(targetPrefix, iteratingName, info) {
			// This is not a match, return
			return
		}

		// We found a match, set <filename> to the iterating name and set <ok> to true
		filename = iteratingName
		ok = true
		// Return break
		return errBreak
	}

	// Iterate through files within directory
	err = walk(p.opts.Dir, fn)
	return
}

func (p *Producer) isWriterMatch(targetPrefix, filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := p.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, p.opts.FullName()+"."+targetPrefix) {
		// We do not have a service match, return
		return
	}

	return true
}

func (p *Producer) waitForNext(s semaphore) {
	select {
	// Wait for semaphore signal
	case <-s:
	// Wait for context to be finished
	case <-p.ctx.Done():
	}
}

func (p *Producer) sleep(d time.Duration) {
	select {
	// Wait for timer duration to complete
	case <-time.NewTimer(d).C:
	// Wait for context to be finished
	case <-p.ctx.Done():
	}
}

func (p *Producer) rename(filename, targetPrefix string, unix int64) (err error) {
	// Set the new name
	newName := fmt.Sprintf("%s.%s.%d.moj", p.opts.FullName(), targetPrefix, unix)
	// Set filename as directory and name joined
	newFilename := path.Join(p.opts.Dir, newName)

	// Rename original filename as new filename
	if err = os.Rename(filename, newFilename); err != nil {
		return
	}

	return
}

func (p *Producer) watch(targetPrefix string, s semaphore, fn func(filename string) error) {
	var (
		filename string

		ok  bool
		err error
	)

	// Iterate until Producer is closed
	for !p.isClosed() {
		// Get next file for the target prefix
		if filename, ok, err = p.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			p.out.Errorf("error getting next %s filename: <%v>, sleeping for a minute and trying again", targetPrefix, err)
			p.sleep(time.Minute)
			continue
		}

		if !ok {
			// No match was found, wait for next signal
			p.waitForNext(s)
			continue
		}

		// Check to see if Producer has closed
		if p.isClosed() {
			break
		}

		// Call provided function
		if err = fn(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			p.out.Errorf("error encountered during action for <%s>: <%v>, sleeping for a minute and trying again", filename, err)
			p.sleep(time.Minute)
		}
	}

	// Decrement jobs waitgroup
	p.jobs.Done()
}

func (p *Producer) handleRemaining(targetPrefix string, fn func(filename string) error) (err error) {
	var (
		filename string
		ok       bool
	)

	for {
		// Get next file for the target prefix
		if filename, ok, err = p.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			err = fmt.Errorf("error getting next file for prefix <%s>: %v", targetPrefix, err)
			return
		}

		if !ok {
			return
		}

		// Call provided function
		if err = fn(filename); err != nil {
			return
		}
	}
}

func (p *Producer) export(filename string) (err error) {
	// Read file and call Exporter.Export
	if err = Read(filename, func(r *Reader) (err error) {
		m := r.Meta()
		// Create the export filename using the service name and the created at value
		// of the current chunp.
		exportFilename := m.generateFilename(p.opts.FullName())
		// Get underlying io.ReadSeeker from Reader
		rs := r.ReadSeeker()
		// Seek to beginning of the file
		if _, err = rs.Seek(0, 0); err != nil {
			return
		}

		// Export file
		// TODO: Utilize a kiroku-level context and pass it here
		if err = p.src.Export(context.Background(), exportFilename, rs); err != nil {
			return
		}

		if meta := r.Meta(); meta.LastSnapshotAt != meta.CreatedAt {
			// This is not a snapshot chunk, return
			return
		}

		// Everything below pertains only to snapshot chunks
		snapshotFilename := getSnapshotName(p.opts.FullName())
		body := strings.NewReader(exportFilename)
		if err = p.src.Export(context.Background(), snapshotFilename, body); err != nil {
			return
		}

		return
	}); err != nil {
		err = fmt.Errorf("error encountered while exporting: %v", err)
		return
	}

	return
}

func (p *Producer) exportAndRemove(filename string) (err error) {
	if !p.hasSource {
		// Exporter not set, return
		return
	}

	// Export file
	if err = p.export(filename); err != nil {
		return
	}

	return p.remove(filename)
}

func (p *Producer) remove(filename string) (err error) {
	return os.Remove(filename)
}

func (p *Producer) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	// Close target chunk
	errs.Push(w.Close())
	// Remove target chunk
	errs.Push(os.Remove(w.filename))
	return errs.Err()
}

func (p *Producer) isClosed() bool {
	select {
	case <-p.ctx.Done():
		// Context done channel is closed, return true
		return true
	default:
		// Context done channel is not closed, return false
		return false
	}
}

func (p *Producer) transaction(fn func(*Writer) error) (err error) {
	// Get current timestamp
	now := time.Now()
	// Get Unix nano value from timestamp
	unix := now.UnixNano()
	// Set name of chunk with temporary prefix
	name := fmt.Sprintf("%s.tmp.chunp.%d", p.opts.FullName(), unix)

	var w *Writer
	// Initialize a new chunk Writer
	if w, err = newWriter(p.opts.Dir, name); err != nil {
		return
	}

	// Since this chunk was freshly created, initialize the chunk Writer
	w.init(&Meta{}, unix)

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

	if w.m.BlockCount == 0 {
		w.Close()
		os.Remove(w.filename)
		return
	}

	return p.importWriter(w)
}

func (p *Producer) importWriter(w *Writer) (err error) {
	// Close transaction chunk
	if err = w.Close(); err != nil {
		err = fmt.Errorf("error closing chunk: %v", err)
		return
	}

	// Rename to chunk with
	if err = p.rename(w.filename, "chunk", time.Now().UnixNano()); err != nil {
		return
	}

	// Send signal to merge watcher
	p.ms.send()
	return
}

func (p *Producer) onChunk(filename string) (err error) {
	// Set current Unix timestamp
	unix := time.Now().UnixNano()

	// Read file and merge into primary chunk
	if err = Read(filename, p.c.Merge); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	// Rename chunk to merged
	if err = p.rename(filename, "merged", unix); err != nil {
		return
	}

	// Send signal to exporting semaphore
	p.es.send()
	return
}

func (p *Producer) onMerge(filename string) (err error) {
	if !p.opts.IsMirror {
		return p.exportAndRemove(filename)
	}

	return p.remove(filename)
}
