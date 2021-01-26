package history

import (
	"fmt"
	"io"
	"os"
)

// Processor will process chunks
type Processor func(m *Meta, r io.ReadSeeker) error

func newProcessorPairFromFile(filename string) (m *Meta, r io.ReadSeeker, err error) {
	var f *os.File
	if f, err = os.Open(filename); err != nil {
		return
	}
	defer f.Close()

	metaBS := make([]byte, metaSize)
	if _, err = io.ReadAtLeast(f, metaBS, int(metaSize)); err != nil {
		err = fmt.Errorf("error reading meta bytes: %v", err)
		return
	}

	m = newMetaFromBytes(metaBS)

	if r, err = newReader(f); err != nil {
		return
	}

	return
}
