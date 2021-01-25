package history

import (
	"io"
	"os"
)

// PostProcessor will process chunks
type PostProcessor func(*Meta, io.Reader) error

type processorPayload struct {
	m *Meta
	r *os.File
}
