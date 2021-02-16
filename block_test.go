package kiroku

import (
	"bytes"
	"testing"

	"github.com/mojura/enkodo"
)

func TestBlock_MarshalEnkodo(t *testing.T) {
	var b Block
	b.Type = TypeComment
	b.Data = []byte("this is a fun comment")

	buf := bytes.NewBuffer(nil)

	var err error
	if err = enkodo.NewWriter(buf).Encode(&b); err != nil {
		t.Fatal(err)
	}
}

func TestBlock_UnmarshalEnkodo(t *testing.T) {
	tcs := []Block{
		{
			Type: TypeComment,
			Data: []byte("this is a fun comment"),
		},
		{
			Type: TypeWriteAction,
			Data: []byte("Write #1"),
		},
		{
			Type: TypeWriteAction,
			Data: []byte("Write #2"),
		},
		{
			Type: TypeDeleteAction,
		},
	}

	var err error
	for _, b := range tcs {
		buf := bytes.NewBuffer(nil)
		if err = enkodo.NewWriter(buf).Encode(&b); err != nil {
			t.Fatal(err)
		}

		var decoded Block
		if err = enkodo.NewReader(buf).Decode(&decoded); err != nil {
			t.Fatal(err)
		}

		switch {
		case decoded.Type != b.Type:
			t.Fatalf("invalid type, expected %v and received %v", b.Type, decoded.Type)
		case bytes.Compare(decoded.Data, b.Data) != 0:
			t.Fatalf("invalid type, expected %v and received %v", string(b.Data), string(decoded.Data))
		}
	}

}
