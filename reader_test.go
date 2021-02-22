package kiroku

import (
	"bytes"
	"fmt"
	"io"
	"log"
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

var testReader *Reader

func TestNewReader(t *testing.T) {
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

	if _, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}
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

func ExampleNewReader() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Open("filename.moj"); err != nil {
		log.Fatalf("error opening: %v", err)
		return
	}

	if testReader, err = NewReader(f); err != nil {
		log.Fatalf("error initializing reader: %v", err)
		return
	}
}

func ExampleReader_Meta() {
	var m Meta
	m = testReader.Meta()
	fmt.Println("Meta!", m)
}

func ExampleReader_ForEach() {
	var err error
	if err = testReader.ForEach(0, func(b *Block) (err error) {
		fmt.Println("Block data:", string(b.Data))
		return
	}); err != nil {
		log.Fatalf("Error iterating through blocks: %v", err)
	}
}

func ExampleReader_Copy() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Create("chunk.copy.moj"); err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if _, err = testReader.Copy(f); err != nil {
		log.Fatalf("Error copying chunk: %v", err)
	}
}

func ExampleReader_CopyBlocks() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Create("chunk.blocksOnly.copy.moj"); err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if _, err = testReader.CopyBlocks(f); err != nil {
		log.Fatalf("Error copying chunk: %v", err)
	}
}

func ExampleRead() {
	var err error
	if err = Read("filename.moj", func(r *Reader) (err error) {
		var m Meta
		m = testReader.Meta()
		fmt.Println("Meta!", m)

		if err = r.ForEach(0, func(b *Block) (err error) {
			fmt.Println("Block data:", string(b.Data))
			return
		}); err != nil {
			log.Fatalf("Error iterating through blocks: %v", err)
		}

		return
	}); err != nil {
		log.Fatal(err)
		return
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
	index uint64

	lastBlockSize int64
}

func populateReaderTestcase(w *Writer, tcs []readerTestcase) (err error) {
	for i, tc := range tcs {
		if err = w.AddBlock(tc.t, []byte(tc.data)); err != nil {
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
