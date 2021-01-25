package history

import "github.com/mojura/enkodo"

// Block represents a block of data stored within history
type Block struct {
	Type Type
	Data []byte
}

// MarshalEnkodo is a enkodo encoding helper func
func (b *Block) MarshalEnkodo(enc *enkodo.Encoder) (err error) {
	enc.Uint8(uint8(b.Type))
	enc.Bytes(b.Data)
	return
}

// UnmarshalEnkodo is a enkodo decoding helper func
func (b *Block) UnmarshalEnkodo(dec *enkodo.Decoder) (err error) {
	var u8 uint8
	if u8, err = dec.Uint8(); err != nil {
		return
	}

	b.Type = Type(u8)

	if err = dec.Bytes(&b.Data); err != nil {
		return
	}

	return
}
