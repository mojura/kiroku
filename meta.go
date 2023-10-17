package kiroku

import "unsafe"

var (
	emptyMeta Meta
	metaSize  = int64(unsafe.Sizeof(Meta{}))
)

func makeMetaFromFilename(filename string) (m Meta, err error) {
	var parsed Filename
	if parsed, err = ParseFilename(filename); err != nil {
		return
	}

	m.LastProcessedTimestamp = parsed.createdAt
	m.LastProcessedType = parsed.filetype
	return
}

func newMetaFromBytes(bs []byte) *Meta {
	// Associate meta with provided bytes
	return (*Meta)(unsafe.Pointer(&bs[0]))
}

// Meta represents the historical meta data
type Meta struct {
	// LastProcessedTimestamp is the last processed timestamp
	LastProcessedTimestamp int64 `json:"lastProcessedTimestamp"`
	LastProcessedType      Type  `json:"type"`
}

func (m *Meta) IsEmpty() bool {
	return *m == emptyMeta
}
