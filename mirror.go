package kiroku

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/gdbu/scribe"
)

// NewMirror will initialize a new Mirror instance
func NewMirror(opts Options, src Source) (mp *Mirror, err error) {
	// Call NewMirrorWithContext with a background context
	return NewMirrorWithContext(context.Background(), opts, src)
}

// NewMirrorWithContext will initialize a new Mirror instance with a provided context.Context
func NewMirrorWithContext(ctx context.Context, opts Options, src Source) (mp *Mirror, err error) {
	var m Mirror
	if m.k, err = NewWithContext(ctx, opts, nil); err != nil {
		return
	}

	scribePrefix := fmt.Sprintf("Kiroku [%s] (Mirror)", opts.Name)
	m.out = scribe.New(scribePrefix)
	m.src = src
	m.opts = opts
	m.ch = make(chan struct{}, 1)
	m.swg.Add(1)
	go m.scan()
	mp = &m
	return
}

// Mirror represents a read-only instance of historical DB entries
// Note: The mirror is updated through it's Importer
type Mirror struct {
	out *scribe.Scribe

	k   *Kiroku
	src Source
	ch  chan struct{}

	opts Options

	swg sync.WaitGroup
}

func (m *Mirror) Channel() <-chan struct{} {
	return m.ch
}

// Meta will return a copy of the current Meta
func (m *Mirror) Meta() (meta Meta, err error) {
	return m.k.Meta()
}

// Filename returns the filename of the primary chunk
func (m *Mirror) Filename() (filename string, err error) {
	return m.k.Filename()
}

func (m *Mirror) Transaction(fn func(*Transaction) error) (err error) {
	return errors.New("mirrors cannot perform transactions")
}

func (m *Mirror) Snapshot(fn func(*Snapshot) error) (err error) {
	return errors.New("mirrors cannot perform snapshots")
}

// Close will close the selected instance of Kiroku
func (m *Mirror) Close() (err error) {
	if err = m.k.Close(); err != nil {
		return
	}

	m.swg.Wait()
	return
}

func (m *Mirror) scan() {
	var (
		lastFile string
		err      error
	)

	defer m.swg.Done()

	if lastFile, err = m.getLastFile(); err != nil {
		m.out.Errorf("error getting last file: %v", err)
		return
	}

	for !m.k.isClosed() {
		if lastFile, err = m.update(lastFile); err != nil {
			m.out.Errorf("error updating: %v", err)
			return
		}
	}
}

func (m *Mirror) getLastFile() (lastFile string, err error) {
	var meta Meta
	if meta, err = m.k.Meta(); err != nil {
		m.out.Errorf("error getting Meta: %v", err)
		return
	}

	if meta.CreatedAt == 0 {
		return
	}

	lastFile = generateFilename(m.k.opts.Name, meta)
	return
}

func (m *Mirror) update(lastFile string) (filename string, err error) {
	prefix := m.k.opts.Name + "."
	filename, err = m.src.GetNext(m.k.ctx, prefix, lastFile)
	switch err {
	case nil:
	case io.EOF:
		err = m.sleep(m.opts.EndOfResultsDelay)
		return

	default:
		err = fmt.Errorf("error getting next: %v", err)
		return
	}

	var f *os.File
	if f, err = m.downloadNext(filename); err != nil {
		return
	}
	defer func() {
		if err == nil {
			return
		}

		err = removeFile(f, m.opts.Dir)
	}()

	if err = m.importWriter(f); err != nil {
		return
	}

	m.notify()
	return
}
func (m *Mirror) downloadNext(filename string) (f *os.File, err error) {
	filepath := path.Join(m.k.opts.Dir, filename)
	if f, err = os.Create(filepath); err != nil {
		err = fmt.Errorf("error creating chunk: %v", err)
		return
	}

	// TODO: Polish this all up
	if err = m.src.Import(m.k.ctx, filename, f); err != nil {
		err = fmt.Errorf("error downloading from source: %v", err)
		return
	}

	if _, err = f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error seeking to beginning of chunk: %v", err)
		return
	}

	return
}

func (m *Mirror) importWriter(f *os.File) (err error) {
	var w *Writer
	if w, err = NewWriterWithFile(f); err != nil {
		err = fmt.Errorf("error creating writer")
		return
	}

	if err = m.k.importWriter(w); err != nil {
		err = fmt.Errorf("error importing writer")
		return
	}

	return
}

func (m *Mirror) sleep(sleepDuration time.Duration) (err error) {
	timer := time.NewTimer(sleepDuration)
	select {
	case <-m.k.ctx.Done():
		timer.Stop()
		return m.k.ctx.Err()
	case <-timer.C:
	}

	return
}

func (m *Mirror) notify() {
	select {
	case m.ch <- struct{}{}:
	default:
	}
}
