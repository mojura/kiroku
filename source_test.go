package kiroku

import (
	"context"
	"fmt"
	"io"
)

func newMockSource(e exportFn, i importFn, g getFn, gn getNextFn) *mockSource {
	var m mockSource
	m.exportFn = e
	m.importFn = i
	m.getFn = g
	m.getNextFn = gn
	fmt.Printf("Mockie: %+v\n", m)
	return &m
}

type mockSource struct {
	exportFn  exportFn
	importFn  importFn
	getFn     getFn
	getNextFn getNextFn
}

func (m *mockSource) Export(ctx context.Context, filename string, r io.Reader) error {
	return m.exportFn(ctx, filename, r)
}

func (m *mockSource) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	return m.importFn(ctx, filename, w)
}

func (m *mockSource) Get(ctx context.Context, filename string, fn func(io.Reader) error) (err error) {
	return m.getFn(ctx, filename, fn)
}

func (m *mockSource) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return m.getNextFn(ctx, prefix, lastFilename)
}

type exportFn func(ctx context.Context, filename string, r io.Reader) error
type importFn func(ctx context.Context, filename string, w io.Writer) error
type getFn func(ctx context.Context, filename string, fn func(io.Reader) error) error
type getNextFn func(ctx context.Context, prefix, lastFilename string) (filename string, err error)

func newErrorSource(srcErr error) *mockSource {
	e := func(ctx context.Context, filename string, r io.Reader) (err error) {
		return srcErr
	}

	i := func(ctx context.Context, filename string, w io.Writer) (err error) {
		return srcErr
	}

	g := func(ctx context.Context, filename string, fn func(io.Reader) error) (err error) {
		return srcErr
	}

	gn := func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
		return "", srcErr
	}

	return newMockSource(e, i, g, gn)
}
