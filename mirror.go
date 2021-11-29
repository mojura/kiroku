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
	go m.scan()
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

func (m *Mirror) scan() {
	var (
		lastFile string
		err      error
	)

	defer m.swg.Done()

	if lastFile, err = m.getNextFile(); err != nil {
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

func (m *Mirror) getNextFile() (nextFile string, err error) {
	for err == nil {
		if nextFile, err = m.k.getNextFile(); err == io.EOF {
			err = m.sleep(m.k.opts.EndOfResultsDelay)
		}

		switch err {
		case nil:
			return
		case io.EOF:
			err = m.sleep(m.k.opts.EndOfResultsDelay)

		default:
			return
		}
	}

	return
}

func (m *Mirror) update(lastFile string) (filename string, err error) {
	prefix := m.k.opts.Name + "."
	filename, err = m.k.src.GetNext(m.k.ctx, prefix, lastFile)
	m.out.Notificationf("Updating: <%s> / <%s> / %v", lastFile, filename, err)

	switch err {
	case nil:
	case io.EOF:
		filename = lastFile
		err = m.sleep(m.k.opts.EndOfResultsDelay)
		return

	default:
		err = fmt.Errorf("error getting next: %v", err)
		return
	}

	if err = m.k.downloadAndImport(filename); err != nil {
		return
	}

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
