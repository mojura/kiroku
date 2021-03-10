package kiroku

import "io"

// Exporter is used for exporting
type Exporter interface {
	Export(filename string, r io.Reader) error
}
