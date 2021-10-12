package kiroku

import (
	"context"
	"io"
)

func newMockSource(e exportFn, i importFn, gn getNextFn, gl getLastSnapshotFn) *mockSource {
	var m mockSource
	m.exportFn = e
	m.importFn = i
	m.getNextFn = gn
	m.getLastSnapshotFn = gl
	return &m
}

type mockSource struct {
	exportFn          exportFn
	importFn          importFn
	getNextFn         getNextFn
	getLastSnapshotFn getLastSnapshotFn
}

func (m *mockSource) Export(ctx context.Context, filename string, r io.Reader) error {
	return m.exportFn(ctx, filename, r)
}

func (m *mockSource) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	return m.importFn(ctx, filename, w)
}

func (m *mockSource) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return m.getNextFn(ctx, prefix, lastFilename)
}

func (m *mockSource) GetLastSnapshot(ctx context.Context, prefix string) (filename string, err error) {
	return m.getLastSnapshotFn(ctx, prefix)
}

type exportFn func(ctx context.Context, filename string, r io.Reader) error
type importFn func(ctx context.Context, filename string, w io.Writer) (err error)
type getNextFn func(ctx context.Context, prefix, lastFilename string) (filename string, err error)
type getLastSnapshotFn func(ctx context.Context, prefix string) (filename string, err error)

func newErrorSource(srcErr error) *mockSource {
	e := func(ctx context.Context, filename string, r io.Reader) (err error) {
		return srcErr
	}

	i := func(ctx context.Context, filename string, w io.Writer) (err error) {
		return srcErr
	}

	gn := func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
		return "", srcErr
	}

	gl := func(ctx context.Context, prefix string) (filename string, err error) {
		return "", srcErr
	}

	return newMockSource(e, i, gn, gl)
}
