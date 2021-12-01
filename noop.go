package kiroku

import (
	"context"
	"fmt"
	"io"
)

var _ Source = &NOOP{}

type NOOP struct {
}

func (n *NOOP) Export(ctx context.Context, filename string, r io.Reader) error {
	return nil
}

func (n *NOOP) Import(ctx context.Context, filename string, w io.Writer) error {
	return fmt.Errorf("file with the name <%s> was not found", filename)
}

func (n *NOOP) Get(ctx context.Context, filename string, fn func(io.Reader) error) error {
	return n.Import(ctx, filename, nil)
}

func (n *NOOP) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return "", io.EOF
}
