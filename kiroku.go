package kiroku

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

// New will initialize a new Kiroku instance
// Note: Processor and Options are optional
func New(o Options, src Source) (kp *Kiroku, err error) {
	// Call NewWithContext with a background context
	return NewWithContext(context.Background(), o, src)
}

// NewWithContext will initialize a new Kiroku instance with a provided context.Context
// Note: Processor and Options are optional
func NewWithContext(ctx context.Context, o Options, src Source) (kp *Kiroku, err error) {
	var k Kiroku
	// Set output prefix
	prefix := fmt.Sprintf("Kiroku (%v)", o.Name)
	// Initialize Kiroku output
	k.out = scribe.New(prefix)
	// Set options
	k.opts = o
	// Set directory as a cleaned version of the provided directory
	k.opts.Dir = filepath.Clean(k.opts.Dir)
	// Initialize cancel context with the provided context as the parent
	k.ctx, k.cancelFn = context.WithCancel(ctx)
	// Set exporter
	// Note: This field is optional and might be nil
	k.src = src
	// Set source state
	k.hasSource = !isNilSource(src)
	// Initialize semaphores
	k.ms = make(semaphore, 1)
	k.es = make(semaphore, 1)

	// Initialize primary Chunk
	if k.c, err = NewWriter(k.opts.Dir, k.opts.Name); err != nil {
		err = fmt.Errorf("error initializing primary chunk: %v", err)
		return
	}

	// Initialize Meta
	if err = k.initMeta(); err != nil {
		err = fmt.Errorf("error initializing meta: %v", err)
		return
	}

	if !k.opts.AvoidImportOnInit {
		if err = k.syncWithSource(); err != nil {
			err = fmt.Errorf("error encountered while syncing with source: %v", err)
			return
		}
	}

	if !k.opts.AvoidMergeOnInit {
		// Options do not request avoiding merge on initialization, merge remaining chunks
		if err = k.handleRemaining("chunk", k.merge); err != nil {
			return
		}
	}

	// Increment jobs waiter
	k.jobs.Add(2)
	// Initialize watch job
	go k.watch("chunk", k.ms, k.merge)
	go k.watch("merged", k.es, k.exportAndRemove)

	// Associate returning pointer to created Kiroku
	kp = &k
	return
}

// Kiroku represents historical DB entries
type Kiroku struct {
	mux sync.RWMutex

	// Output logger
	out *scribe.Scribe

	// Kiroku options
	opts Options

	src       Source
	hasSource bool

	// Goroutine job waiter
	jobs sync.WaitGroup

	// Last meta
	m Meta
	// Merging semaphore
	ms semaphore
	// Exporting semaphore
	es semaphore

	// Primary chunk
	c *Writer

	// Context of history
	ctx context.Context
	// Context cancel func
	cancelFn func()
}

// Meta will return a copy of the current Meta
func (k *Kiroku) Meta() (m Meta, err error) {
	k.mux.RLock()
	defer k.mux.RUnlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		err = errors.ErrIsClosed
		return
	}

	m = k.m
	return
}

// Transaction will engage a new history transaction
func (k *Kiroku) Transaction(fn func(*Transaction) error) (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()
	// Check to see if Kiroku is closed
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	txnFn := func(w *Writer) (err error) {
		txn := newTransaction(w)

		// Call provided function
		return fn(txn)
	}

	return k.transaction(txnFn)
}

// Snapshot will engage a new history snapshot
func (k *Kiroku) Snapshot(fn func(*Snapshot) error) (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	txnFn := func(w *Writer) (err error) {
		// Initialize snapshot
		ss := newSnapshot(w)
		// Call provided function
		return fn(ss)
	}

	return k.transaction(txnFn)
}

// Filename returns the filename of the primary chunk
func (k *Kiroku) Filename() (filename string, err error) {
	k.mux.RLock()
	defer k.mux.RUnlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		err = errors.ErrIsClosed
		return
	}

	filename = k.c.filename
	return
}

// Close will close the selected instance of Kiroku
func (k *Kiroku) Close() (err error) {
	k.mux.Lock()
	defer k.mux.Unlock()

	// Check to see if Kiroku is closed
	if k.isClosed() {
		return errors.ErrIsClosed
	}

	// Cancel the context
	k.cancelFn()

	// Wait for jobs to finish
	k.jobs.Wait()

	var errs errors.ErrorList
	if !k.opts.AvoidMergeOnClose {
		// Options do not request avoiding merge on close, merge remaining chunks
		errs.Push(k.handleRemaining("chunk", k.merge))
	}

	if !k.opts.AvoidExportOnClose {
		// Options do not request avoiding merge on close, process remaining merged chunks
		errs.Push(k.handleRemaining("merged", k.exportAndRemove))
	}

	// Close primary chunk
	errs.Push(k.c.Close())
	return errs.Err()
}

func (k *Kiroku) initMeta() (err error) {
	var (
		filename string
		ok       bool
	)

	if !k.m.isEmpty() {
		return
	}

	// Get last chunk
	filename, ok, err = k.getLast("chunk")
	switch {
	case err != nil:
		// Error encountered, return
		return
	case !ok:
		// No chunks found, set Meta as the Meta from the primary chunk
		k.m = *k.c.m
		return
	default:
		// Read last chunk and set Meta from the reader
		return Read(filename, k.setMetaFromReader)
	}
}

func (k *Kiroku) setMetaFromReader(r *Reader) (err error) {
	// Set underlying Meta as the Reader's Meta
	k.m = r.Meta()
	return
}

func (k *Kiroku) getTruncatedName(filename string) (name string) {
	// Truncate name by removing directory
	// TODO: There might have been a reason it was setup this way instead of using
	// path.Base, unfortunately I forgot to leave a comment as to why I did so. This
	// is a note to do some discovery around this. The outcome should be one of two:
	//	1. Comment as to why this approach was used
	//	2. Use path.Base
	return strings.Replace(filename, k.opts.Dir+"/", "", 1)
}

func (k *Kiroku) getNext(targetPrefix string) (filename string, ok bool, err error) {
	fn := func(iteratingName string, info os.FileInfo) (err error) {
		// Check to see if current file is a match for the current name and prefix
		if !k.isWriterMatch(targetPrefix, iteratingName, info) {
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
	err = walk(k.opts.Dir, fn)
	return
}

func (k *Kiroku) getLast(targetPrefix string) (filename string, ok bool, err error) {
	fn := func(iteratingName string, info os.FileInfo) (err error) {
		isMatch := k.isWriterMatch(targetPrefix, iteratingName, info)
		switch {
		case !isMatch && !ok:
			// We do not have a match, and we have not matched yet. Return and search
			// for more!
			return
		case !isMatch && ok:
			// We do not have a match, and we have matched before. We have exceeded
			// the range of our prefix. Return errBreak
			return errBreak

		default:
			// We found a match, set <filename> to the iterating name and set <ok> to true
			filename = iteratingName
			ok = true
			return
		}
	}

	// Iterate through files within directory
	err = walk(k.opts.Dir, fn)
	return
}

func (k *Kiroku) isWriterMatch(targetPrefix, filename string, info os.FileInfo) (ok bool) {
	if info.IsDir() {
		// We are not interested in directories, return
		return
	}

	// Get truncated name
	name := k.getTruncatedName(filename)

	// Check to see if filename has the needed prefix
	if !strings.HasPrefix(name, k.opts.Name+"."+targetPrefix) {
		// We do not have a service match, return
		return
	}

	return true
}

func (k *Kiroku) waitForNext(s semaphore) {
	select {
	// Wait for semaphore signal
	case <-s:
	// Wait for context to be finished
	case <-k.ctx.Done():
	}
}

func (k *Kiroku) sleep(d time.Duration) {
	select {
	// Wait for timer duration to complete
	case <-time.NewTimer(d).C:
	// Wait for context to be finished
	case <-k.ctx.Done():
	}
}

func (k *Kiroku) rename(filename, targetPrefix string, unix int64) (err error) {
	// Set the new name
	newName := fmt.Sprintf("%s.%s.%d.moj", k.opts.Name, targetPrefix, unix)
	// Set filename as directory and name joined
	newFilename := path.Join(k.opts.Dir, newName)

	// Rename original filename as new filename
	if err = os.Rename(filename, newFilename); err != nil {
		return
	}

	return
}

func (k *Kiroku) watch(targetPrefix string, s semaphore, fn func(filename string) error) {
	var (
		filename string

		ok  bool
		err error
	)

	// Iterate until Kiroku is closed
	for !k.isClosed() {
		// Get next file for the target prefix
		if filename, ok, err = k.getNext(targetPrefix); err != nil {
			// TODO: Get teams input on if this value should be configurable
			k.out.Errorf("error getting next %s filename: <%v>, sleeping for a minute and trying again", targetPrefix, err)
			k.sleep(time.Minute)
			continue
		}

		if !ok {
			// No match was found, wait for next signal
			k.waitForNext(s)
			continue
		}

		// Check to see if Kiroku has closed
		if k.isClosed() {
			break
		}

		// Call provided function
		if err = fn(filename); err != nil {
			// TODO: Get teams input on the best course of action here
			k.out.Errorf("error encountered during action for <%s>: <%v>, sleeping for a minute and trying again", filename, err)
			k.sleep(time.Minute)
		}
	}

	// Decrement jobs waitgroup
	k.jobs.Done()
}

func (k *Kiroku) handleRemaining(targetPrefix string, fn func(filename string) error) (err error) {
	var (
		filename string
		ok       bool
	)

	for {
		// Get next file for the target prefix
		if filename, ok, err = k.getNext(targetPrefix); err != nil {
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

func (k *Kiroku) merge(filename string) (err error) {
	// Set current Unix timestamp
	unix := time.Now().UnixNano()

	// Read file and merge into primary chunk
	if err = Read(filename, k.c.Merge); err != nil {
		err = fmt.Errorf("error encountered while merging: %v", err)
		return
	}

	// Rename chunk to merged
	if err = k.rename(filename, "merged", unix); err != nil {
		return
	}

	// Send signal to exporting semaphore
	k.es.send()
	return
}

func (k *Kiroku) export(filename string) (err error) {
	if !k.hasSource {
		// Exporter not set, return
		return
	}

	// Read file and call Exporter.Export
	if err = Read(filename, func(r *Reader) (err error) {
		m := r.Meta()
		// Create the export filename using the service name and the created at value
		// of the current chunk.
		exportFilename := m.generateFilename(k.opts.Name)
		// Get underlying io.ReadSeeker from Reader
		rs := r.ReadSeeker()
		// Seek to beginning of the file
		if _, err = rs.Seek(0, 0); err != nil {
			return
		}

		// Export file
		// TODO: Utilize a kiroku-level context and pass it here
		if err = k.src.Export(context.Background(), exportFilename, rs); err != nil {
			return
		}

		if meta := r.Meta(); meta.LastSnapshotAt != meta.CreatedAt {
			// This is not a snapshot chunk, return
			return
		}

		// Everything below pertains only to snapshot chunks
		snapshotFilename := getSnapshotName(k.opts.Name)
		body := strings.NewReader(exportFilename)
		if err = k.src.Export(context.Background(), snapshotFilename, body); err != nil {
			return
		}

		return
	}); err != nil {
		err = fmt.Errorf("error encountered while exporting: %v", err)
		return
	}

	return
}

func (k *Kiroku) exportAndRemove(filename string) (err error) {
	// Export file
	if err = k.export(filename); err != nil {
		return
	}

	// Remove file
	if err = os.Remove(filename); err != nil {
		return
	}

	return
}

func (k *Kiroku) deleteChunk(w *Writer) (err error) {
	var errs errors.ErrorList
	// Close target chunk
	errs.Push(w.Close())
	// Remove target chunk
	errs.Push(os.Remove(w.filename))
	return errs.Err()
}

func (k *Kiroku) isClosed() bool {
	select {
	case <-k.ctx.Done():
		// Context done channel is closed, return true
		return true
	default:
		// Context done channel is not closed, return false
		return false
	}
}

func (k *Kiroku) transaction(fn func(*Writer) error) (err error) {
	// Get current timestamp
	now := time.Now()
	// Get Unix nano value from timestamp
	unix := now.UnixNano()
	// Set name of chunk with temporary prefix
	name := fmt.Sprintf("%s.tmp.chunk.%d", k.opts.Name, unix)

	var w *Writer
	// Initialize a new chunk Writer
	if w, err = newWriter(k.opts.Dir, name); err != nil {
		return
	}

	// Since this chunk was freshly created, initialize the chunk Writer
	w.init(&k.m, unix)

	// Call provided function
	if err = fn(w); err != nil {
		// Error encountered, delete chunk!
		if deleteErr := k.deleteChunk(w); deleteErr != nil {
			// Error encountered while deleting chunk, leave error log to notify server manager
			k.out.Errorf("error deleting chunk <%s>: %v", name, deleteErr)
		}

		// Return error from provided function
		return
	}

	if w.m.BlockCount == k.m.BlockCount {
		w.Close()
		os.Remove(w.filename)
		return
	}

	return k.importWriter(w)
}

func (k *Kiroku) importWriter(w *Writer) (err error) {
	// Get Meta from transaction chunk
	newMeta := *w.m

	// Close transaction chunk
	if err = w.Close(); err != nil {
		err = fmt.Errorf("error closing chunk: %v", err)
		return
	}

	// Rename to chunk with
	if err = k.rename(w.filename, "chunk", newMeta.CreatedAt); err != nil {
		return
	}

	// Send signal to merge watcher
	k.ms.send()

	// Set underlying Meta as the transaction chunk's Meta
	k.m = newMeta
	return
}

func (k *Kiroku) download(filename string) (f *os.File, err error) {
	filepath := path.Join(k.opts.Dir, filename)
	if f, err = os.Create(filepath); err != nil {
		err = fmt.Errorf("error creating chunk: %v", err)
		return
	}

	// TODO: Polish this all up
	if err = k.src.Import(k.ctx, filename, f); err != nil {
		err = fmt.Errorf("error downloading from source: %v", err)
		return
	}

	if _, err = f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error seeking to beginning of chunk: %v", err)
		return
	}

	return
}

func (k *Kiroku) downloadAndImport(filename string) (err error) {
	var f *os.File
	if f, err = k.download(filename); err != nil {
		err = fmt.Errorf("error downloading <%s>: %v", filename, err)
		return
	}
	defer func() {
		if err == nil {
			f.Close()
			return
		}

		if err := removeFile(f, k.opts.Dir); err != nil {
			k.out.Errorf("error removing file <")
		}
	}()

	var w *Writer
	if w, err = NewWriterWithFile(f); err != nil {
		err = fmt.Errorf("error creating writer")
		return
	}

	if err = k.importWriter(w); err != nil {
		err = fmt.Errorf("error importing writer")
		return
	}

	return
}

func (k *Kiroku) syncWithSource() (err error) {
	if !k.hasSource {
		return
	}

	prefix := k.opts.Name + "."
	for nextFile, err := k.getInitialSyncFile(); err == nil; nextFile, err = k.syncWithFile(prefix, nextFile) {
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (k *Kiroku) syncWithFile(prefix, filename string) (nextFile string, err error) {
	if err = k.downloadAndImport(filename); err != nil {
		err = fmt.Errorf("error during download and import: %v", err)
		return
	}

	nextFile, err = k.src.GetNext(k.ctx, prefix, filename)
	switch err {
	case nil:
		return
	case io.EOF:
		return
	default:
		err = fmt.Errorf("error while getting next: %v", err)
		return
	}
}

func (k *Kiroku) getInitialSyncFile() (nextFile string, err error) {
	var meta Meta
	if meta, err = k.Meta(); err != nil {
		err = fmt.Errorf("error getting Meta: %v", err)
		return
	}

	var latestSnapshot string
	latestSnapshot, err = k.getLatestSnapshotFilename()
	switch err {
	case nil:
		var parsed filenameMeta
		if parsed, err = parseFilename(latestSnapshot); err != nil {
			return
		}

		if meta.CreatedAt < parsed.createdAt {
			nextFile = latestSnapshot
			return
		}

	case os.ErrNotExist:
		// Snapshot doesn't exist, continue on in attempt to get next file

	default:
		return
	}

	return k.getNextFile()
}

func (k *Kiroku) getLatestSnapshotFilename() (filename string, err error) {
	snapshotFilename := getSnapshotName(k.opts.Name)
	err = k.src.Get(k.ctx, snapshotFilename, func(r io.Reader) (err error) {
		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, r)
		switch err {
		case nil:
			filename = buf.String()
			return
		case io.EOF:
			return nil

		default:
			return
		}
	})

	return
}

func (k *Kiroku) getNextFile() (nextFile string, err error) {
	var meta Meta
	if meta, err = k.Meta(); err != nil {
		err = fmt.Errorf("error getting Meta: %v", err)
		return
	}

	var currentFile string
	if meta.BlockCount > 0 {
		currentFile = meta.generateFilename(k.opts.Name)
	}

	return k.src.GetNext(k.ctx, k.opts.Name+".", currentFile)
}
