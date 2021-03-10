package kiroku

import (
	"context"
	"io"
)

// Importer is used for importing
type Importer interface {
	ImportNext(ctx context.Context, prefix, lastFilename string, w io.WriterAt) (filename string, err error)
}
