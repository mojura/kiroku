package kiroku

import "github.com/mojura/enkodo"

// Block represents a block of data stored within history
type Block []byte

// MarshalEnkodo is a enkodo encoding helper func
func (b Block) MarshalEnkodo(enc *enkodo.Encoder) (err error) {
	// Write key as bytes
	if err = enc.Bytes(b); err != nil {
		return
	}

	return
}

// UnmarshalEnkodo is a enkodo decoding helper func
func (b *Block) UnmarshalEnkodo(dec *enkodo.Decoder) (err error) {
	var bs []byte
	// Decode key as bytes
	if err = dec.Bytes(&bs); err != nil {
		return
	}

	*b = bs
	return
}
