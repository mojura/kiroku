package kiroku

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/gdbu/scribe"
)

// NewMirror will initialize a new Mirror instance
func NewMirror(dir, name string, i Importer, opts *MirrorOptions) (mp *Mirror, err error) {
	// Call NewMirrorWithContext with a background context
	return NewMirrorWithContext(context.Background(), dir, name, i, opts)
}

// NewMirrorWithContext will initialize a new Mirror instance with a provided context.Context
func NewMirrorWithContext(ctx context.Context, dir, name string, i Importer, opts *MirrorOptions) (mp *Mirror, err error) {
	var m Mirror
	if m.k, err = NewWithContext(ctx, dir, name, nil, &opts.Options); err != nil {
		return
	}

	scribePrefix := fmt.Sprintf("Kiroku [%s] (Mirror)", name)
	m.out = scribe.New(scribePrefix)
	m.i = i
	m.opts = opts
	m.swg.Add(1)
	go m.scan()
	mp = &m
	return
}

// Mirror represents a read-only instance of historical DB entries
// Note: The mirror is updated through it's Importer
type Mirror struct {
	out *scribe.Scribe

	k *Kiroku
	i Importer

	opts *MirrorOptions

	swg sync.WaitGroup
}

func (m *Mirror) scan() {
	var (
		meta     Meta
		lastFile string
		err      error
	)

	defer m.swg.Done()

	if meta, err = m.k.Meta(); err != nil {
		m.out.Errorf("error getting Meta: %v", err)
		return
	}

	prefix := m.k.name + "."

	if meta.CreatedAt > 0 {
		lastFile = generateFilename(m.k.name, meta.CreatedAt)
	}

	for !m.k.isClosed() {
		var filename string
		filename, err = m.i.GetNext(m.k.ctx, prefix, lastFile)
		switch err {
		case nil:
		case io.EOF:
			time.Sleep(time.Second * 10)

		default:
			m.out.Debugf("GetNext error: %v", err)
		}

		filepath := path.Join(m.k.dir, filename)

		var f *os.File
		if f, err = os.Create(filepath); err != nil {
			m.out.Debugf("Error creating file: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		// TODO: Polish this all up
		if err = m.i.Import(m.k.ctx, filename, f); err != nil {
			return
		}

		if _, err = f.Seek(0, 0); err != nil {
			return
		}

		var w *Writer
		if w, err = NewWriterWithFile(f); err != nil {
			return
		}

		if err = m.k.importWriter(w); err != nil {
			return
		}
	}
}

// Meta will return a copy of the current Meta
func (m *Mirror) Meta() (meta Meta, err error) {
	return m.k.Meta()
}

// Filename returns the filename of the primary chunk
func (m *Mirror) Filename() (filename string, err error) {
	return m.k.Filename()
}

// Close will close the selected instance of Kiroku
func (m *Mirror) Close() (err error) {
	if err = m.k.Close(); err != nil {
		return
	}

	m.swg.Wait()
	return
}
