package history

import (
	"fmt"
	"io"
	"os"
)

// Processor will process chunks
type Processor func(*Reader) error

func newProcessorPair(filename string) (m *Meta, f *os.File, err error) {
	if f, err = os.Open(filename); err != nil {
		return
	}

	var info os.FileInfo
	if info, err = f.Stat(); err != nil {
		err = fmt.Errorf("error getting file info for <%s>: %v", filename, err)
		return
	}

	if info.Size() < metaSize {
		err = fmt.Errorf("file size for <%s> of %d bytes is smaller than the minimum of %d", filename, info.Size(), metaSize)
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
