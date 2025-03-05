package kiroku

import (
	"context"
	"io"
	"os"
)

var _ Source = &NOOP{}

type NOOP struct {
}

func (n *NOOP) Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error) {
	return filename, nil
}

func (n *NOOP) Import(ctx context.Context, prefix, filename string, w io.Writer) error {
	return os.ErrNotExist
}

func (n *NOOP) Get(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
	return n.Import(ctx, filename, prefix, nil)
}

func (n *NOOP) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return "", io.EOF
}

func (n *NOOP) GetNextList(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
	return []string{}, io.EOF
}

func (n *NOOP) GetInfo(ctx context.Context, prefix, filename string) (Info, error) {
	return Info{}, io.EOF
}
