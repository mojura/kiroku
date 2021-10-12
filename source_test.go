package kiroku

import (
	"context"
	"io"
)

func newMockSource(e exportFn, g getNextFn, i importFn) *mockSource {
	var m mockSource
	m.getNextFn = g
	m.importFn = i
	return &m
}

type mockSource struct {
	exportFn  exportFn
	getNextFn getNextFn
	importFn  importFn
}

func (m *mockSource) Export(filename string, r io.Reader) error {
	return m.exportFn(filename, r)
}

func (m *mockSource) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return m.getNextFn(ctx, prefix, lastFilename)
}

func (m *mockSource) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	return m.importFn(ctx, filename, w)
}

type exportFn func(filename string, r io.Reader) error
type getNextFn func(ctx context.Context, prefix, lastFilename string) (filename string, err error)
type importFn func(ctx context.Context, filename string, w io.Writer) (err error)

func newErrorSource(srcErr error) *mockSource {
	e := func(filename string, r io.Reader) (err error) {
		return srcErr
	}

	g := func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
		return "", srcErr
	}

	i := func(ctx context.Context, filename string, w io.Writer) (err error) {

		return srcErr
	}

	return newMockSource(e, g, i)
}
