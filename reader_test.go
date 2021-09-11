package kiroku

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

var readerTestcases = [4]readerTestcase{
	{
		t:     TypeWriteAction,
		key:   "key1",
		value: "foo 1",
		index: 1,
	},
	{
		t:     TypeWriteAction,
		key:   "key2",
		value: "foo 2",
		index: 2,
	},
	{
		t:     TypeWriteAction,
		key:   "key3",
		value: "foo 3",
		index: 3,
	},
	{
		t:     TypeDeleteAction,
		key:   "key3",
		value: "",
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

func TestNewReader_with_closed_file(t *testing.T) {
	var (
		f   *os.File
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if f, err = os.OpenFile("./test_data/test", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0744); err != nil {
		t.Fatal(err)
	}

	// Close file to trigger error
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	// Set expected error as a seek error due to the file being closed
	expectedErr := fmt.Errorf("error encountered while seeking to beginning of file: seek %s: file already closed", "./test_data/test")

	// Attempt to initialize Reader
	_, err = NewReader(f)

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestNewReader_with_append_only_file(t *testing.T) {
	var (
		f   *os.File
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if f, err = os.OpenFile("./test_data/test", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0744); err != nil {
		t.Fatal(err)
	}

	// Write contents to file to force seek
	if _, err = f.WriteString("test"); err != nil {
		t.Fatal(err)
	}

	// Set expected error as an io.EOF bubbling out to reader initialization
	expectedErr := fmt.Errorf("error reading meta bytes: %v", io.ErrUnexpectedEOF)

	// Attempt to initialize Reader
	_, err = NewReader(f)

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	if err = populateReaderTestcase(c, tcs[:]); err != nil {
		t.Fatal(err)
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

	if _, err = testForEach(r, tcs[:], 0); err != nil {
		t.Fatal(err)
	}
}

func TestReader_ForEach_with_active_writes(t *testing.T) {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	if err = populateReaderTestcase(c, tcs[:2]); err != nil {
		t.Fatal(err)
	}

	var lastPosition int64
	if err = Read(c.Filename(), func(r *Reader) (err error) {
		if lastPosition, err = testForEach(r, tcs[:2], 0); err != nil {
			t.Fatal(err)
		}

		return

	}); err != nil {
		return
	}

	if err = populateReaderTestcase(c, tcs[2:]); err != nil {
		t.Fatal(err)
	}

	c.mm.Flush()

	if err = Read(c.Filename(), func(r *Reader) (err error) {
		if _, err = testForEach(r, tcs[2:], lastPosition); err != nil {
			t.Fatal(err)
		}

		return

	}); err != nil {
		return
	}
}

func TestReader_ForEach_with_seek_error(t *testing.T) {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

	if err = c.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("read %s: file already closed", c.filename)

	_, err = r.ForEach(0, func(b *Block) (err error) {
		return
	})

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestReader_ForEach_with_processor_error(t *testing.T) {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

	expectedErr := errors.Error("foobar")

	_, err = r.ForEach(0, func(b *Block) (err error) {
		return expectedErr
	})

	if err = compareErrors(expectedErr, err); err != nil {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

	if _, err = testForEach(cr, tcs[:], 0); err != nil {
		t.Fatal(err)
	}
}

func TestReader_Copy_with_seek_error(t *testing.T) {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

	if err = c.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("read %s: file already closed", c.filename)

	_, err = r.Copy(bytes.NewBuffer(nil))

	if err = compareErrors(expectedErr, err); err != nil {
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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
		if str := string(b.Value); str != tc.value {
			t.Fatalf("invalid data, expected <%s> and received <%s>", tc.value, str)
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

	if c, err = NewWriter("./test_data", "testie"); err != nil {
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

		_, err = testForEach(r, tcs[:], 0)
		return
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRead_invalid_file(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	expectedErr := fmt.Errorf("open ./test_data/testie: no such file or directory")
	err = Read("./test_data/testie", func(r *Reader) (err error) {
		return
	})

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestRead_non_readable_file(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.OpenFile("./test_data/test", os.O_CREATE|os.O_RDWR, 0511); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("error reading meta bytes: %v", io.EOF)
	err = Read("./test_data/test", func(r *Reader) (err error) {
		return
	})

	if err = compareErrors(expectedErr, err); err != nil {
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
	m := testReader.Meta()
	fmt.Println("Meta!", m)
}

func ExampleReader_ForEach() {
	var (
		lastPosition int64
		err          error
	)

	if lastPosition, err = testReader.ForEach(0, func(b *Block) (err error) {
		fmt.Println("Block value:", string(b.Value))
		return
	}); err != nil {
		log.Fatalf("Error iterating through blocks: %v", err)
	}

	fmt.Println("Last read block at", lastPosition)
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
		m := testReader.Meta()
		fmt.Println("Meta!", m)

		if _, err = r.ForEach(0, func(b *Block) (err error) {
			fmt.Println("Block value:", string(b.Value))
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

func testForEach(r *Reader, tcs []readerTestcase, seek int64) (lastPosition int64, err error) {
	var count int
	if lastPosition, err = r.ForEach(seek, func(b *Block) (err error) {
		tc := tcs[count]
		if str := string(b.Key); str != tc.key {
			err = fmt.Errorf("invalid key, expected <%s> and received <%s>", tc.key, str)
			return
		}

		if str := string(b.Value); str != tc.value {
			err = fmt.Errorf("invalid value, expected <%s> and received <%s>", tc.value, str)
			return
		}

		count++
		return
	}); err != nil {
		err = fmt.Errorf("error during iteration: %v", err)
		return
	}

	if expectedTotal := len(tcs); count != expectedTotal {
		err = fmt.Errorf("invalid number of iterations, expected %d and received %d", expectedTotal, count)
		return
	}

	lastPosition = r.Meta().TotalBlockSize
	return
}

type readerTestcase struct {
	t     Type
	key   string
	value string
	index uint64

	lastBlockSize int64
}

func populateReaderTestcase(w *Writer, tcs []readerTestcase) (err error) {
	for i, tc := range tcs {
		if err = w.AddBlock(tc.t, []byte(tc.key), []byte(tc.value)); err != nil {
			err = fmt.Errorf("error adding row: %v", err)
			return
		}

		if err = w.SetIndex(tc.index); err != nil {
			return
		}

		tcs[i].lastBlockSize = w.m.TotalBlockSize
	}

	return
}
