package kiroku

import (
	"context"
	"io"
)

// Source is used for importing
type Source interface {
	Export(filename string, r io.Reader) error
	Import(ctx context.Context, filename string, w io.Writer) (err error)
	GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error)
}
