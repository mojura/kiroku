package history

import (
	"fmt"
	"io"
	"os"
)

// Processor will process chunks
type Processor func(m *Meta, r io.ReadSeeker) error

func newProcessorPairFromFile(filename string) (m *Meta, f *os.File, err error) {
	if f, err = os.Open(filename); err != nil {
		return
	}

	metaBS := make([]byte, metaSize)
	if _, err = io.ReadAtLeast(f, metaBS, int(metaSize)); err != nil {
		err = fmt.Errorf("error reading meta bytes: %v", err)
		return
	}

	m = newMetaFromBytes(metaBS)
	return
}
