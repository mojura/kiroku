package kiroku

import (
	"context"
	"io"
)

// Importer is used for importing
type Importer interface {
	GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error)
	Import(ctx context.Context, filename string, w io.Writer) (err error)
}
