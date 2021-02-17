package kiroku

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/mojura/enkodo"
)

var readerTestcases = []readerTestcase{
	{
		t:     TypeWriteAction,
		data:  "foo 1",
		index: 1,
	},
	{
		t:     TypeWriteAction,
		data:  "foo 2",
		index: 2,
	},
	{
		t:     TypeWriteAction,
		data:  "foo 3",
		index: 3,
	},
	{
		t:     TypeDeleteAction,
		data:  "",
		index: 3,
	},
}

func TestReader_Meta(t *testing.T) {
	var (
		c   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if c, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	if err = populateReaderTestcase(c, tcs); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	meta := r.Meta()
	if last := tcs[len(tcs)-1]; meta.CurrentIndex != last.index {
		t.Fatalf("invalid index, expected %d and received %d", last.index, meta.CurrentIndex)
	}

	if meta.BlockCount != int64(len(tcs)) {
		t.Fatalf("invalid row count, expected %d and received %d", len(tcs), meta.BlockCount)
	}
}

func TestReader_ForEach(t *testing.T) {
	var (
		c   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if c, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	if err = populateReaderTestcase(c, tcs); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	var count int
	if err = r.ForEach(0, func(b *Block) (err error) {
		tc := tcs[count]
		if str := string(b.Data); str != tc.data {
			err = fmt.Errorf("invalid data, expected <%s> and received <%s>", tc.data, str)
			return
		}

		count++
		return
	}); err != nil {
		t.Fatalf("error during iteration: %v", err)
		return
	}

	if count != len(tcs) {
		t.Fatalf("invalid number of iterations, expected %d and received %d", len(tcs), count)
	}
}

func TestReader_Copy(t *testing.T) {
	var (
		c   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if c, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = populateReaderTestcase(c, tcs); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
	}

	buf := bytes.NewBuffer(nil)

	if _, err = r.Copy(buf); err != nil {
		t.Fatalf("error copying to buffer: %v", err)
	}

	var cr *Reader
	if cr, err = NewReader(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("error initializing reader: %v", err)
	}

	var count int
	if err = cr.ForEach(0, func(b *Block) (err error) {
		tc := tcs[count]
		if str := string(b.Data); str != tc.data {
			err = fmt.Errorf("invalid data, expected <%s> and received <%s>", tc.data, str)
			return
		}

		count++
		return
	}); err != nil {
		t.Fatalf("error during iteration: %v", err)
	}

	if count != len(tcs) {
		t.Fatalf("invalid number of iterations, expected %d and received %d", len(tcs), count)
	}
}

func TestReader_CopyBlocks(t *testing.T) {
	var (
		c   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if c, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	if err = populateReaderTestcase(c, tcs); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	buf := bytes.NewBuffer(nil)

	if _, err = r.CopyBlocks(buf); err != nil {
		t.Fatalf("error copying to buffer: %v", err)
	}

	dec := enkodo.NewReader(buf)

	var count int
	for err == nil {
		var b Block
		if err = dec.Decode(&b); err != nil {
			break
		}

		tc := tcs[count]
		if str := string(b.Data); str != tc.data {
			t.Fatalf("invalid data, expected <%s> and received <%s>", tc.data, str)
		}

		count++
	}

	switch err {
	case nil:
	case io.EOF:

	default:
		t.Fatal(err)
	}

	if count != len(tcs) {
		t.Fatalf("invalid number of iterations, expected %d and received %d", len(tcs), count)
	}
}

type readerTestcase struct {
	t     Type
	data  string
	index int64
}

func populateReaderTestcase(w *Writer, tcs []readerTestcase) (err error) {
	for _, tc := range tcs {
		if err = w.AddRow(tc.t, []byte(tc.data)); err != nil {
			err = fmt.Errorf("error adding row: %v", err)
			return
		}

		w.SetIndex(tc.index)
	}

	if _, err = w.f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error setting file to start: %v", err)
		return
	}

	return
}
