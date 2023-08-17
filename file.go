package kiroku

import "io"

type File interface {
	io.Seeker
	io.Reader
	io.ReaderAt
}
