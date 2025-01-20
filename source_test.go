package kiroku

import (
	"context"
	"io"
)

var _ Source = &mockSource{}

func newMockSource(e exportFn, i importFn, g getFn, gn getNextFn, gnl getNextListFn, gh getInfoFn) *mockSource {
	var m mockSource
	m.exportFn = e
	m.importFn = i
	m.getFn = g
	m.getNextFn = gn
	m.getNextListFn = gnl
	m.getInfoFn = gh
	return &m
}

type mockSource struct {
	exportFn      exportFn
	importFn      importFn
	getFn         getFn
	getNextFn     getNextFn
	getNextListFn getNextListFn
	getInfoFn     getInfoFn
}

func (m *mockSource) Export(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
	return m.exportFn(ctx, prefix, filename, r)
}

func (m *mockSource) Import(ctx context.Context, prefix, filename string, w io.Writer) (err error) {
	return m.importFn(ctx, prefix, filename, w)
}

func (m *mockSource) Get(ctx context.Context, prefix, filename string, fn func(io.Reader) error) (err error) {
	return m.getFn(ctx, prefix, filename, fn)
}

func (m *mockSource) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return m.getNextFn(ctx, prefix, lastFilename)
}

func (m *mockSource) GetNextList(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
	return m.getNextListFn(ctx, prefix, lastFilename, maxKeys)
}

func (m *mockSource) GetInfo(ctx context.Context, prefix, filename string) (Info, error) {
	return m.getInfoFn(ctx, prefix, filename)
}

type exportFn func(ctx context.Context, prefix, filename string, r io.Reader) (string, error)
type importFn func(ctx context.Context, prefix, filename string, w io.Writer) error
type getFn func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error
type getNextFn func(ctx context.Context, prefix, lastFilename string) (filename string, err error)
type getNextListFn func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error)
type getInfoFn func(ctx context.Context, prefix, filename string) (Info, error)
