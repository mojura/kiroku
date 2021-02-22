package kiroku

import (
	"bytes"
	"testing"

	"github.com/mojura/enkodo"
)

func TestBlock_MarshalEnkodo(t *testing.T) {
	var b Block
	b.Type = TypeComment
	b.Key = []byte("testKey")
	b.Value = []byte("this is a fun comment")

	buf := bytes.NewBuffer(nil)

	var err error
	if err = enkodo.NewWriter(buf).Encode(&b); err != nil {
		t.Fatal(err)
	}
}

func TestBlock_UnmarshalEnkodo(t *testing.T) {
	tcs := []Block{
		{
			Type:  TypeComment,
			Value: []byte("this is a fun comment"),
		},
		{
			Type:  TypeWriteAction,
			Key:   []byte("key1"),
			Value: []byte("Write #1"),
		},
		{
			Type:  TypeWriteAction,
			Key:   []byte("key2"),
			Value: []byte("Write #2"),
		},
		{
			Key:  []byte("key2"),
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
		case !bytes.Equal(decoded.Key, b.Key):
			t.Fatalf("invalid key, expected %v and received %v", string(b.Key), string(decoded.Key))
		case !bytes.Equal(decoded.Value, b.Value):
			t.Fatalf("invalid value, expected %v and received %v", string(b.Value), string(decoded.Value))
		}
	}

}
