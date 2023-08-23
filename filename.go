package kiroku

import (
	"fmt"
	"strconv"
	"strings"
)

func makeFilename(name string, createdAt int64, filetype Type) (f Filename) {
	f.name = name
	f.createdAt = createdAt
	f.filetype = filetype
	return
}

func parseFilename(filename string) (parsed Filename, err error) {
	spl := strings.Split(filename, ".")
	if len(spl) != 4 {
		err = fmt.Errorf("invalid number of filename parts, expected 4 and received %d", len(spl))
		return
	}

	if parsed.createdAt, err = strconv.ParseInt(spl[1], 10, 64); err != nil {
		return
	}

	parsed.name = spl[0]
	if parsed.filetype, err = parseType(spl[2]); err != nil {
		return
	}

	return
}

type Filename struct {
	name      string
	createdAt int64
	filetype  Type
}

func (f Filename) String() string {
	return fmt.Sprintf("%s.%d.%s.kir", f.name, f.createdAt, f.filetype)
}

func (f Filename) toMeta() (m Meta) {
	m.LastProcessedTimestamp = f.createdAt
	m.LastProcessedType = f.filetype
	return
}
