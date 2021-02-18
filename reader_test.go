package kiroku

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/mojura/enkodo"
)

var readerTestcases = [4]readerTestcase{
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

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	if err = testMeta(r, tcs[:]); err != nil {
		t.Fatal(err)
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

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	if err = testForEach(r, tcs[:]); err != nil {
		t.Fatal(err)
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

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
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

	if err = testForEach(cr, tcs[:]); err != nil {
		t.Fatal(err)
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

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
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

func TestRead(t *testing.T) {
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

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
		t.Fatal(err)
	}

	if err = Read(c.filename, func(r *Reader) (err error) {
		if err = testMeta(r, tcs[:]); err != nil {
			return
		}

		return testForEach(r, tcs[:])
	}); err != nil {
		t.Fatal(err)
	}
}

func testMeta(r *Reader, tcs []readerTestcase) (err error) {
	meta := r.Meta()
	if last := tcs[len(tcs)-1]; meta.CurrentIndex != last.index {
		return fmt.Errorf("invalid index, expected %d and received %d", last.index, meta.CurrentIndex)
	}

	if meta.BlockCount != int64(len(tcs)) {
		return fmt.Errorf("invalid row count, expected %d and received %d", len(tcs), meta.BlockCount)
	}

	return
}

func testForEach(r *Reader, tcs []readerTestcase) (err error) {
	var lastBlockSize int64
	for i := 0; i < len(tcs); i++ {
		var count int
		if err = r.ForEach(lastBlockSize, func(b *Block) (err error) {
			tc := tcs[count+i]
			if str := string(b.Data); str != tc.data {
				err = fmt.Errorf("invalid data, expected <%s> and received <%s>", tc.data, str)
				return
			}

			count++
			return
		}); err != nil {
			err = fmt.Errorf("error during iteration: %v", err)
			return
		}

		if expectedTotal := len(tcs) - i; count != expectedTotal {
			err = fmt.Errorf("invalid number of iterations, expected %d and received %d", expectedTotal, count)
			return
		}

		lastBlockSize = tcs[i].lastBlockSize
	}

	return
}

type readerTestcase struct {
	t     Type
	data  string
	index int64

	lastBlockSize int64
}

func populateReaderTestcase(w *Writer, tcs []readerTestcase) (err error) {
	for i, tc := range tcs {
		if err = w.AddRow(tc.t, []byte(tc.data)); err != nil {
			err = fmt.Errorf("error adding row: %v", err)
			return
		}

		w.SetIndex(tc.index)
		tcs[i].lastBlockSize = w.m.TotalBlockSize
	}

	if _, err = w.f.Seek(0, 0); err != nil {
		err = fmt.Errorf("error setting file to start: %v", err)
		return
	}

	return
}
