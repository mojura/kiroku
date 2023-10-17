package kiroku

import (
	"fmt"
	"strconv"
	"strings"
)

func makeFilename(name string, createdAt int64, filetype Type) (f Filename) {
	f.Name = name
	f.CreatedAt = createdAt
	f.Filetype = filetype
	return
}

func ParseFilename(filename string) (parsed Filename, err error) {
	spl := strings.Split(filename, ".")
	if len(spl) != 4 {
		err = fmt.Errorf("invalid number of filename parts, expected 4 and received %d", len(spl))
		return
	}

	if parsed.CreatedAt, err = strconv.ParseInt(spl[1], 10, 64); err != nil {
		return
	}

	parsed.Name = spl[0]
	if parsed.Filetype, err = parseType(spl[2]); err != nil {
		return
	}

	if spl[3] != "kir" {
		err = fmt.Errorf("invalid file extension, expected <%s> and received <%s>", "kir", spl[3])
		return
	}

	return
}

type Filename struct {
	Name      string
	CreatedAt int64
	Filetype  Type
}

func (f Filename) String() string {
	return fmt.Sprintf("%s.%d.%s.kir", f.Name, f.CreatedAt, f.Filetype)
}

func (f Filename) toMeta() (m Meta) {
	m.LastProcessedTimestamp = f.CreatedAt
	m.LastProcessedType = f.Filetype
	return
}
