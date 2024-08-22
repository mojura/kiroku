package kiroku

import (
	"context"
	"io"
)

// Source is used for importing
type Source interface {
	Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error)
	Import(ctx context.Context, prefix, filename string, w io.Writer) error
	Get(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error
	GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error)
	GetNextList(ctx context.Context, prefix, lastFilename string, maxkeys int64) (nextKeys []string, err error)
}
