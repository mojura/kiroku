package kiroku

import "unsafe"

var (
	emptyMeta Meta
	metaSize  = int64(unsafe.Sizeof(Meta{}))
)

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

func (m *Meta) merge(in *Meta) {
	// Check to see if inbound Meta exists
	if in == nil {
		// Inbound Meta does not exist, bail out
		return
	}

	mm := *in
	// Set the underlying Meta as the dereferenced value of the inbound Meta
	*m = mm
}

func (m *Meta) isEmpty() bool {
	return *m == emptyMeta
}
