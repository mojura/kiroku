package kiroku

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"

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
func NewConsumer(opts Options, src Source, onUpdate UpdateFunc) (mp *Consumer, err error) {
	// Call NewConsumerWithContext with a background context
	return NewConsumerWithContext(context.Background(), opts, src, onUpdate)
}

// NewConsumerWithContext will initialize a new Consumer instance with a provided context.Context
func NewConsumerWithContext(ctx context.Context, opts Options, src Source, onUpdate UpdateFunc) (c *Consumer, err error) {
	if c, err = newConsumer(ctx, opts, src, onUpdate); err != nil {
		return
	}

	c.swg.Add(c.opts.ConsumerConcurrencyCount)
	for i := 0; i < c.opts.ConsumerConcurrencyCount; i++ {
		go c.scan()
	}

	return
}

// NewOneShotConsumer will initialize a new one-shot Consumer instance with a provided context.Context
func NewOneShotConsumer(opts Options, src Source, onUpdate UpdateFunc) (err error) {
	ctx := context.Background()
	return NewOneShotConsumerWithContext(ctx, opts, src, onUpdate)
}

// NewConsumerWithContext will initialize a new Consumer instance with a provided context.Context
func NewOneShotConsumerWithContext(ctx context.Context, opts Options, src Source, onUpdate UpdateFunc) (err error) {
	var c *Consumer
	if c, err = newConsumer(ctx, opts, src, onUpdate); err != nil {
		return
	}

	if err = c.oneShot(); err != nil {
		return
	}

	return c.Close()
}

func newConsumer(ctx context.Context, opts Options, src Source, onUpdate UpdateFunc) (ref *Consumer, err error) {
	var c Consumer
	if err = opts.Validate(); err != nil {
		return
	}

	if c.m, err = newMappedMeta(opts); err != nil {
		return
	}

	rangeStart := opts.RangeStart.UnixNano() - 1
	if err = c.m.Update(func(meta Meta) (out Meta, err error) {
		// Set the last processed values as the last downloaded values if the values are set
		// This will ensure that any downloads that did not complete will be re-tried
		// when this scan process begins
		if meta.LastDownloadedTimestamp > 0 {
			meta.LastProcessedTimestamp = meta.LastDownloadedTimestamp
			meta.LastProcessedType = meta.LastDownloadedType
		}

		// Ensure the last processed timestamp is not less than the range start
		if meta.LastProcessedTimestamp < rangeStart {
			meta.LastProcessedTimestamp = rangeStart
		}

		out = meta
		return
	}); err != nil {
		return
	}

	c.ctx, c.close = context.WithCancel(ctx)
	c.opts = opts
	c.src = src
	c.onUpdate = onUpdate
	if c.queueLength, err = c.getQueueLength(); err != nil {
		return
	}

	c.w = newWatcher(c.ctx, c.opts, c.onChunk, TypeChunk, TypeSnapshot)
	ref = &c
	return
}

// Consumer represents a read-only instance of historical DB entries
// Note: The mirror is updated through it's Importer
type Consumer struct {
	ctx   context.Context
	close func()

	m *mappedMeta

	w *watcher

	// Queue length is only used when capacity is set
	queueLength int64

	// List of filenames to download from, when this is empty - more can be replenished
	// Note: This is only accessed during mappedMeta.Update, this func is what protects
	// thread safety for filenames
	f filenames

	opts     Options
	src      Source
	onUpdate UpdateFunc

	swg sync.WaitGroup
}

// Meta will return a copy of the current Meta
func (c *Consumer) Meta() (meta Meta, err error) {
	if isClosed(c.ctx) {
		err = errors.ErrIsClosed
		return
	}

	meta = c.m.Get()
	return
}

// Close will close the selected instance of Kiroku
func (c *Consumer) Close() (err error) {
	if isClosed(c.ctx) {
		c.w.waitToComplete()
		return errors.ErrIsClosed
	}

	c.close()
	c.w.waitToComplete()
	if err = c.w.processAll(); err != nil {
		return
	}

	return c.m.Close()
}

func (c *Consumer) scan() {
	var err error
	c.swg.Add(1)
	defer c.swg.Done()
	if err = c.getLatestSnapshot(); err != nil {
		err = fmt.Errorf("Consumer.scan(): error getting latest snapshot: %v", err)
		c.opts.OnError(err)
		return
	}

	var hasError bool
	resume := func() {}
	if c.opts.OnResume != nil {
		resume = func() {
			if !hasError {
				return
			}

			hasError = false
			go c.opts.OnResume()
		}
	}

	for err == nil && !isClosed(c.ctx) {
		err = c.sync()
		switch err {
		case nil:
			resume()
		case io.EOF:
			if c.opts.Debugging {
				fmt.Println("End of results found, sleeping")
			}

			resume()
			err = sleep(c.ctx, c.opts.EndOfResultsDelay)
		default:
			err = fmt.Errorf("Consumer.scan(): error updating: %v", err)
			c.opts.OnError(err)
			hasError = true
			err = sleep(c.ctx, c.opts.ErrorDelay)
		}
	}
}

func (c *Consumer) sync() (err error) {
	for err == nil && !isClosed(c.ctx) {
		err = c.getNext()
	}

	return
}

func (c *Consumer) oneShot() (err error) {
	if err = c.getLatestSnapshot(); err != nil {
		err = fmt.Errorf("error getting latest snapshot: %v", err)
		return
	}

	err = c.sync()
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
	}

	return
}

func (c *Consumer) isWithinCapcity() (ok bool, err error) {
	if c.opts.ConsumerFileLimit <= 0 {
		return true, nil
	}

	if c.queueLength >= c.opts.ConsumerFileLimit {
		if c.queueLength, err = c.getQueueLength(); err != nil {
			return
		}
	}

	ok = c.queueLength < c.opts.ConsumerFileLimit
	c.queueLength++

	if c.opts.Debugging {
		fmt.Printf("Consumer[%s].isWithinCapacity(): Length %d / Limit %d / Ok %v\n", c.opts.FullName(), c.queueLength, c.opts.ConsumerFileLimit, ok)
	}

	return
}

func (c *Consumer) getQueueLength() (n int64, err error) {
	err = filepath.Walk(c.opts.Dir, func(path string, info fs.FileInfo, ierr error) (err error) {
		if ierr != nil {
			log.Printf("error opening iterating file: %v\n", ierr)
			return nil
		}

		var fn Filename
		base := filepath.Base(path)
		if fn, err = ParseFilename(base); err != nil {
			return nil
		}

		if fn.Name != c.opts.FullName() {
			return nil
		}

		n++
		return nil
	})

	return
}

func (c *Consumer) getNextFilename(meta Meta) (filename string, err error) {
	var ok bool
	if filename, ok = c.f.Shift(); ok {
		return
	}

	// Our filelist is empty, so we need to repopulate it.
	// Determine the filename of our last processed file by using the last processed timestamp and type
	lastFile := makeFilename(c.opts.FullName(), meta.LastProcessedTimestamp, meta.LastProcessedType)

	var filenames []string
	// Get next batch of filenames starting from immediately after the last file we processed
	filenames, err = c.src.GetNextList(c.ctx, c.opts.FullName(), lastFile.String(), c.opts.ConsumerGetNextListSize)
	switch err {
	case nil:
		c.f.Append(filenames)
		if filename, ok = c.f.Shift(); !ok {
			err = io.EOF
		}

		return
	case io.EOF:
		return

	default:
		err = fmt.Errorf("error getting next list: %v", err)
		return
	}
}

func (c *Consumer) getNext() (err error) {
	var ok bool
	if ok, err = c.isWithinCapcity(); err != nil {
		return
	} else if !ok {
		return io.EOF
	}

	var filename string
	if err = c.m.Update(func(meta Meta) (out Meta, err error) {
		if filename, err = c.getNextFilename(meta); err != nil {
			return
		}

		var parsed Filename
		if parsed, err = ParseFilename(filename); err != nil {
			return
		}

		var inRange bool
		if inRange, err = c.isWithinRange(parsed); err != nil {
			err = fmt.Errorf("error checking if filename <%s> is within range: %v", filename, err)
			return
		}

		if !inRange {
			err = io.EOF
			return
		}

		// Set last processed
		meta.LastProcessedTimestamp = parsed.CreatedAt
		meta.LastProcessedType = parsed.Filetype
		out = meta
		return
	}); err != nil {
		return
	}

	if err = c.download(filename); err != nil {
		err = fmt.Errorf("error downloading <%s>: %v", filename, err)
		return
	}

	return
}

func (c *Consumer) isWithinRange(filename Filename) (inRange bool, err error) {
	if c.opts.RangeEnd.IsZero() {
		return true, nil
	}

	rangeEnd := c.opts.RangeEnd.UnixNano()
	inRange = rangeEnd >= filename.CreatedAt
	return
}

func (c *Consumer) shouldDownload(latestSnapshot string) (should bool, err error) {
	if len(latestSnapshot) == 0 {
		return
	}

	var meta Meta
	if meta, err = c.Meta(); err != nil {
		return
	}

	var parsed Filename
	if parsed, err = ParseFilename(latestSnapshot); err != nil {
		err = fmt.Errorf("error determining if should download snapshot <%s>: %v", latestSnapshot, err)
		return
	}

	if !c.opts.RangeEnd.IsZero() && c.opts.RangeEnd.UnixNano() < parsed.CreatedAt {
		return
	}

	// If the latest snapshot timestamp is after the last processed timestamp, we should download
	return meta.LastProcessedTimestamp < parsed.CreatedAt, nil
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

	var should bool
	if should, err = c.shouldDownload(latestSnapshot); err != nil || !should {
		return
	}

	if err = c.download(latestSnapshot); err != nil {
		return fmt.Errorf("error downloading initial snapshot <%s>: %v", latestSnapshot, err)
	}

	return
}

func (c *Consumer) getLatestSnapshotFilename() (filename string, err error) {
	snapshotFilename := getSnapshotName(c.opts.FullName())
	err = c.src.Get(c.ctx, "_latestSnapshots", snapshotFilename, func(r io.Reader) (err error) {
		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, r)
		switch err {
		case nil, io.EOF:
			filename = buf.String()
			return nil

		default:
			return
		}
	})

	return
}

func (c *Consumer) download(filename string) (err error) {
	var tmpFilepath string
	c.opts.OnLog(fmt.Sprintf("downloading <%s>", filename))
	defer c.opts.OnLog(fmt.Sprintf("downloaded <%s>", filename))
	if tmpFilepath, err = c.downloadTemp(filename); err != nil {
		return
	}
	// Always ensure temporary file is deleted after this function is over
	// Note: If there are no errors through this func, this will technically fail
	// due to the tmp filepath being renamed
	defer os.Remove(tmpFilepath)

	filepath := path.Join(c.opts.Dir, filename)
	if err = renameFile(tmpFilepath, filepath); err != nil {
		err = fmt.Errorf("error renaming temporary file: %v", err)
		return
	}

	var fnm Filename
	if fnm, err = ParseFilename(filename); err != nil {
		err = fmt.Errorf("error parsing filename <%s>: %v", filename, err)
		return
	}

	c.m.SetDownloaded(fnm.CreatedAt, fnm.Filetype)
	c.w.trigger()
	return
}

func (c *Consumer) downloadTemp(filename string) (tmpFilepath string, err error) {
	var tmp *os.File
	tmpFilepath = path.Join(c.opts.Dir, "_downloading."+filename)
	if tmp, err = createFile(tmpFilepath); err != nil {
		err = fmt.Errorf("error creating chunk: %v", err)
		return
	}
	defer tmp.Close()

	if err = c.src.Import(c.ctx, c.opts.FullName(), filename, tmp); err != nil {
		err = fmt.Errorf("error downloading from source: %v", err)
		return
	}

	return
}

func (c *Consumer) onChunk(filename Filename) (err error) {
	// Process chunk
	filepath := path.Join(c.opts.Dir, filename.String())
	if err = Read(filepath, func(r *Reader) (err error) {
		return c.onUpdate(filename.Filetype, r)
	}); err != nil {
		err = fmt.Errorf("error encountered while processing: %v", err)
		return
	}

	if err = os.Remove(filepath); err != nil {
		err = fmt.Errorf("error encountered while removing processed file <%s>: %v", filename, err)
		return
	}

	return
}
