package kiroku

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gdbu/scribe"
	"github.com/hatchify/errors"
)

const (
	// ErrMirrorNilSource is returned when a mirror is initialized with a nil source
	ErrMirrorNilSource = errors.Error("mirrors cannot have a nil source")
	// ErrMirrorTransaction is returned when a mirror attempts a transaction
	ErrMirrorTransaction = errors.Error("mirrors cannot perform transactions")
	// ErrMirrorSnapshot is returned when a mirror attempts to snapshot
	ErrMirrorSnapshot = errors.Error("mirrors cannot perform snapshots")
)

// NewMirror will initialize a new Mirror instance
func NewMirror(opts Options, src Source) (mp *Mirror, err error) {
	// Call NewMirrorWithContext with a background context
	return NewMirrorWithContext(context.Background(), opts, src)
}

// NewMirrorWithContext will initialize a new Mirror instance with a provided context.Context
func NewMirrorWithContext(ctx context.Context, opts Options, src Source) (mp *Mirror, err error) {
	var m Mirror
	if m.k, err = NewWithContext(ctx, opts, src); err != nil {
		return
	}

	if !m.k.hasSource {
		err = ErrMirrorNilSource
		return
	}

	scribePrefix := fmt.Sprintf("Kiroku [%s] (Mirror)", opts.Name)
	m.out = scribe.New(scribePrefix)
	m.ch = make(chan struct{}, 1)
	m.swg.Add(1)

	var nextFile string
	if nextFile, err = m.init(); err != nil {
		return
	}

	go m.scan(nextFile)
	mp = &m
	return
}

// Mirror represents a read-only instance of historical DB entries
// Note: The mirror is updated through it's Importer
type Mirror struct {
	out *scribe.Scribe

	k  *Kiroku
	ch chan struct{}

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
	return ErrMirrorNilSource
}

func (m *Mirror) Snapshot(fn func(*Snapshot) error) (err error) {
	return ErrMirrorSnapshot
}

// Close will close the selected instance of Kiroku
func (m *Mirror) Close() (err error) {
	if err = m.k.Close(); err != nil {
		return
	}

	m.swg.Wait()
	return
}

func (m *Mirror) init() (nextFile string, err error) {
	nextFile, err = m.k.getNextFile()
	switch err {
	case nil:
	case io.EOF:
	default:
		return
	}

	for !m.k.isClosed() {
		nextFile, err = m.update(nextFile)
		switch err {
		case nil:
		case io.EOF:
			err = nil
			return

		default:
			return
		}
	}

	return
}

func (m *Mirror) scan(nextFile string) {
	var err error
	defer m.swg.Done()
	for err == nil && !m.k.isClosed() {
		nextFile, err = m.update(nextFile)
		switch err {
		case nil:
		case io.EOF:
			err = m.sleep(m.k.opts.EndOfResultsDelay)

		default:
			m.out.Errorf("error updating: %v", err)
			err = m.sleep(m.k.opts.EndOfResultsDelay)
		}
	}
}

func (m *Mirror) update(lastFile string) (filename string, err error) {
	prefix := m.k.opts.FullName() + "."
	filename, err = m.k.src.GetNext(m.k.ctx, prefix, lastFile)

	switch err {
	case nil:
	case io.EOF:
		filename = lastFile
		return

	default:
		err = fmt.Errorf("error getting next: %v", err)
		return
	}

	m.out.Notificationf("downloading <%s>", filename)

	if err = m.k.downloadAndImport(filename); err != nil {
		return
	}

	m.out.Notificationf("downloaded <%s>", filename)
	m.notify()
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
