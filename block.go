package kiroku

import "github.com/mojura/enkodo"

// Block represents a block of data stored within history
type Block struct {
	// Type of block
	Type Type
	// Key of block
	Key []byte
	// Value of block
	Value []byte
}

// MarshalEnkodo is a enkodo encoding helper func
func (b *Block) MarshalEnkodo(enc *enkodo.Encoder) (err error) {
	// Write type as uint8
	if err = enc.Uint8(uint8(b.Type)); err != nil {
		return
	}

	// Write key as bytes
	if err = enc.Bytes(b.Key); err != nil {
		return
	}

	// Write value as bytes
	if err = enc.Bytes(b.Value); err != nil {
		return
	}

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

	// Decode key as bytes
	if err = dec.Bytes(&b.Key); err != nil {
		return
	}

	// Decode value as bytes
	if err = dec.Bytes(&b.Value); err != nil {
		return
	}

	return
}
