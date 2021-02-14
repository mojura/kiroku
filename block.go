package history

import "github.com/mojura/enkodo"

// Block represents a block of data stored within history
type Block struct {
	// Type of block
	Type Type
	// Data of block
	Data []byte
}

// MarshalEnkodo is a enkodo encoding helper func
func (b *Block) MarshalEnkodo(enc *enkodo.Encoder) (err error) {
	// Write type as uint8
	enc.Uint8(uint8(b.Type))
	// Write data as bytes
	enc.Bytes(b.Data)
	return
}

// UnmarshalEnkodo is a enkodo decoding helper func
func (b *Block) UnmarshalEnkodo(dec *enkodo.Decoder) (err error) {
	var u8 uint8
	// Decode uint8 value
	if u8, err = dec.Uint8(); err != nil {
		return
	}

	// Convert uint8 value to Type
	b.Type = Type(u8)

	// Decode data as bytes
	if err = dec.Bytes(&b.Data); err != nil {
		return
	}

	return
}
