package kiroku

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hatchify/errors"
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

func TestBlock_MarshalEnkodo_with_error(t *testing.T) {
	var b Block
	b.Type = TypeComment
	b.Key = []byte("testKey")
	b.Value = []byte("this is a fun comment")

	tcs := []countWriter{
		{
			count: 1,
			err:   io.EOF,
		},
		{
			count: 2,
			err:   io.EOF,
		},
		{
			count: 3,
			err:   io.EOF,
		},
		{
			count: 4,
			err:   nil,
		},
	}

	for _, tc := range tcs {
		if err := enkodo.NewWriter(&tc).Encode(&b); err != tc.err {
			t.Fatalf("invalid error, expected <%v> and received <%v>", tc.err, err)
		}
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

func TestBlock_UnmarshalEnkodo_with_errors(t *testing.T) {
	var b Block
	b.Type = TypeWriteAction
	b.Key = []byte("key")
	b.Value = []byte("value")

	buf := bytes.NewBuffer(nil)
	if err := enkodo.NewWriter(buf).Encode(&b); err != nil {
		t.Fatal(err)
	}

	targetErr := errors.Error("foo bar!")
	tcs := []countReader{
		{
			r:     bytes.NewReader(buf.Bytes()),
			count: 1,
			err:   targetErr,
		},
		{
			r:     bytes.NewReader(buf.Bytes()),
			count: 2,
			err:   targetErr,
		},
		{
			r:     bytes.NewReader(buf.Bytes()),
			count: 3,
			err:   targetErr,
		},
		{
			r:     bytes.NewReader(buf.Bytes()),
			count: 4,
			err:   targetErr,
		},
		{
			r:     bytes.NewReader(buf.Bytes()),
			count: 5,
			err:   nil,
		},
	}

	for i, tc := range tcs {
		err := enkodo.NewReader(&tc).Decode(&b)
		fmt.Println("Hmm", err, tc.err)
		switch {
		case tc.err == nil && err == nil:
		case tc.err == err:
		case tc.err == nil && err != nil:
			t.Fatalf("invalid error, expected <nil> and received <%v>", err)
		case tc.err != nil && err == nil:
			t.Fatalf("invalid error, expected <%v> and received <nil>", tc.err)
		case tc.err != nil && !strings.Contains(err.Error(), tc.err.Error()):
			t.Fatalf("invalid error, expected to contain <%v> and received <%v> (test case #%d)", tc.err, err, i)
		}
	}
}

type countWriter struct {
	n     int
	count int

	err error
}

func (w *countWriter) Write(bs []byte) (n int, err error) {
	if w.n++; w.n >= w.count {
		err = w.err
		return
	}

	n = len(bs)
	return
}

type countReader struct {
	r *bytes.Reader

	n     int
	count int

	err error
}

func (r *countReader) Read(bs []byte) (n int, err error) {
	fmt.Println("READ", r.n)
	if r.n++; r.n >= r.count {
		err = r.err
		return
	}

	n, err = r.r.Read(bs)
	return
}

func (r *countReader) ReadByte() (b byte, err error) {
	fmt.Println("READ", r.n)
	if r.n++; r.n >= r.count {
		err = r.err
		return
	}

	return r.r.ReadByte()
}
