package kiroku

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

const (
	// ErrConsumerNilSource is returned when a mirror is initialized with a nil source
	ErrConsumerNilSource = errors.Error("mirrors cannot have a nil source")
	// ErrConsumerTransaction is returned when a mirror attempts a transaction
	ErrConsumerTransaction = errors.Error("mirrors cannot perform transactions")
	// ErrConsumerSnapshot is returned when a mirror attempts to snapshot
	ErrConsumerSnapshot = errors.Error("mirrors cannot perform snapshots")
)

// NewConsumer will initialize a new Consumer instance
func NewConsumer(opts Options, src Source, onUpdate func(*Reader) error) (mp *Consumer, err error) {
	// Call NewConsumerWithContext with a background context
	return NewConsumerWithContext(context.Background(), opts, src, onUpdate)
}

// NewConsumerWithContext will initialize a new Consumer instance with a provided context.Context
func NewConsumerWithContext(ctx context.Context, opts Options, src Source, onUpdate func(*Reader) error) (ref *Consumer, err error) {
	var c Consumer
	scribePrefix := fmt.Sprintf("Kiroku [%s] (Consumer)", opts.Name)
	c.ctx, c.close = context.WithCancel(ctx)
	c.out = scribe.New(scribePrefix)

	// Initialize semaphores
	c.onUpdate = onUpdate

	c.w = newWatcher(c.ctx, c.opts, c.out, "chunk", c.onChunk)
	c.swg.Add(1)

	go c.scan()
	ref = &c
	return
}

// Consumer represents a read-only instance of historical DB entries
// Note: The mirror is updated through it's Importer
type Consumer struct {
	mux sync.RWMutex

	ctx   context.Context
	close func()

	m Meta

	out *scribe.Scribe
	w   *watcher

	name     string
	opts     Options
	src      Source
	onUpdate func(*Reader) error

	swg sync.WaitGroup
}

// Meta will return a copy of the current Meta
func (c *Consumer) Meta() (meta Meta, err error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	meta = c.m
	return
}

func (c *Consumer) setMeta(meta Meta) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.m.LastProcessedTimestamp = meta.LastProcessedTimestamp
	c.m.LastProcessedType = meta.LastProcessedType
	return
}

// Close will close the selected instance of Kiroku
func (c *Consumer) Close() (err error) {
	c.close()
	c.w.waitToComplete()
	return
}

func (c *Consumer) scan() {
	var err error
	defer c.swg.Done()
	if err = c.getLatestSnapshot(); err != nil {
		c.out.Errorf("Consumer.scan(): error getting latest snapshot: %v", err)
		return
	}

	for err == nil && !isClosed(c.ctx) {
		err = c.getNext()
		switch err {
		case nil:
		case io.EOF:
			err = sleep(c.ctx, c.opts.EndOfResultsDelay)

		default:
			c.out.Errorf("Consumer.scan(): error updating: %v", err)
			err = sleep(c.ctx, c.opts.ErrorDelay)
		}
	}
}

func (c *Consumer) getNext() (err error) {
	prefix := c.opts.FullName() + "."
	var meta Meta
	if meta, err = c.Meta(); err != nil {
		return
	}

	var filename string
	lastFile := makeFilename(c.opts.FullName(), meta.LastProcessedTimestamp, meta.LastProcessedType)
	filename, err = c.src.GetNext(c.ctx, prefix, lastFile.String())

	switch err {
	case nil:
	case io.EOF:
		filename = lastFile.String()
		return

	default:
		err = fmt.Errorf("error getting next: %v", err)
		return
	}

	if err = c.download(filename); err != nil {
		err = fmt.Errorf("error downloading <%s>: %v", err)
		return
	}

	return
}

func (c *Consumer) getLatestSnapshot() (err error) {
	var latestSnapshot string
	latestSnapshot, err = c.getLatestSnapshotFilename()
	switch err {
	case nil:
	case os.ErrNotExist:
		err = nil
		return

	default:
		return
	}

	var meta Meta
	if meta, err = c.Meta(); err != nil {
		return
	}

	var after bool
	if after, err = wasCreatedAfter(latestSnapshot, meta.LastProcessedTimestamp); err != nil {
		err = fmt.Errorf("error determining if snapshot <%s> was created after %v: %v", latestSnapshot, meta.LastProcessedTimestamp, err)
		return
	}

	if !after {
		return
	}

	if err = c.download(latestSnapshot); err != nil {
		return fmt.Errorf("error downloading initial snapshot <%s>: %v", latestSnapshot, err)
	}

	return
}

func (c *Consumer) getLatestSnapshotFilename() (filename string, err error) {
	snapshotFilename := getSnapshotName(c.opts.FullName())
	err = c.src.Get(c.ctx, snapshotFilename, func(r io.Reader) (err error) {
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

func (c *Consumer) download(filename string) (err error) {
	var tmpFilepath string
	c.out.Notificationf("downloading <%s>", filename)
	defer c.out.Notificationf("downloaded <%s>", filename)
	if tmpFilepath, err = c.downloadTemp(filename); err != nil {
		return
	}
	// Always ensure temporary file is deleted after this function is over
	// Note: If there are no errors through this func, this will technically fail
	// due to the tmp filepath being renamed
	defer os.Remove(tmpFilepath)

	filepath := path.Join(c.opts.Dir, filename)
	if err = os.Rename(tmpFilepath, filepath); err != nil {
		err = fmt.Errorf("error renaming temporary file: %v", err)
		return
	}

	var fnm Filename
	if fnm, err = parseFilename(filename); err != nil {
		err = fmt.Errorf("error parsing filename <%s>: %v", filename, err)
		return
	}

	c.setMeta(fnm.toMeta())
	c.w.trigger()
	return
}

func (c *Consumer) downloadTemp(filename string) (tmpFilepath string, err error) {
	var tmp *os.File
	//	tmpFilepath = path.Join(os.TempDir(), "_downloading."+filename)
	tmpFilepath = path.Join(c.opts.Dir, "_downloading."+filename)
	if tmp, err = os.Create(tmpFilepath); err != nil {
		err = fmt.Errorf("error creating chunk: %v", err)
		return
	}
	defer tmp.Close()

	if err = c.src.Import(c.ctx, filename, tmp); err != nil {
		err = fmt.Errorf("error downloading from source: %v", err)
		return
	}

	return
}

func (c *Consumer) onChunk(filename string) (err error) {
	// Process chunk
	if err = Read(filename, c.onUpdate); err != nil {
		err = fmt.Errorf("error encountered while processing: %v", err)
		return
	}

	if err = os.Remove(filename); err != nil {
		err = fmt.Errorf("error encountered while removing processed file <%s>: %v", filename, err)
		return
	}

	return
}
