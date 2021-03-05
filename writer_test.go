package kiroku

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/hatchify/errors"
)

var testWriter *Writer

func Test_NewWriter(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	type testcase struct {
		dir  string
		name string

		expectedError error
	}

	tcs := []testcase{
		{
			dir:           "./test_data",
			name:          "testie",
			expectedError: nil,
		},
		{
			dir:           "./foobar_no_dir",
			name:          "testie",
			expectedError: fmt.Errorf(`open foobar_no_dir/testie.moj: no such file or directory`),
		},
	}

	for _, tc := range tcs {
		_, err = NewWriter(tc.dir, tc.name)
		if err = compareErrors(tc.expectedError, err); err != nil {
			t.Fatal(err)
		}
	}
}

func Test_NewWriterWithFile(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	var rdOnlyFile *os.File
	if rdOnlyFile, err = os.OpenFile("./test_data/rdOnly_testfile.txt", os.O_CREATE|os.O_RDONLY, 0744); err != nil {
		t.Fatal(err)
	}

	var appendOnlyFile *os.File
	if appendOnlyFile, err = os.OpenFile("./test_data/appendOnly_testfile.txt", os.O_CREATE|os.O_APPEND, 0744); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		f *os.File

		expectedError error
	}

	truncateErrLayout := "error mapping Meta: error setting file size to %d: truncate ./test_data/%s: %v"
	tcs := []testcase{
		{
			f:             rdOnlyFile,
			expectedError: fmt.Errorf(truncateErrLayout, metaSize, "rdOnly_testfile.txt", os.ErrInvalid),
		},
		{
			f:             appendOnlyFile,
			expectedError: fmt.Errorf(truncateErrLayout, metaSize, "appendOnly_testfile.txt", os.ErrInvalid),
		},
	}

	/*

		<error mapping Meta: error setting file size to 40: truncate rdOnly_testfile.txt: invalid argument>
		<error mapping Meta: error setting file size to 40: truncate ./test_data/rdOnly_testfile.txt: invalid argument> (test case #0)
	*/

	for i, tc := range tcs {
		_, err = NewWriterWithFile(tc.f)
		if err = compareErrors(tc.expectedError, err); err != nil {
			t.Fatalf("%v (test case #%d)", err, i)
		}
	}
}

func TestWriter_GetIndex(t *testing.T) {
	testSetIndexGetIndex(t)
}

func TestWriter_GetIndex_on_closed(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = w.GetIndex(); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestWriter_SetIndex(t *testing.T) {
	testSetIndexGetIndex(t)
}

func TestWriter_SetIndex_on_closed(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	if err = w.SetIndex(1337); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestWriter_NextIndex(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	for i := uint64(0); i < 100; i++ {
		var index uint64
		index, err = w.NextIndex()
		switch {
		case err != nil:
			t.Fatal(err)
		case index != i:
			t.Fatalf("invalid index, expected %d and received %d", i, index)
		}
	}
}

func TestWriter_NextIndex_on_closed(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = w.NextIndex(); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestWriter_AddBlock(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	for _, tc := range tcs {
		if err = w.AddBlock(tc.t, []byte(tc.key), []byte(tc.value)); err != nil {
			t.Fatalf("error adding row: %v", err)
		}
	}
}

func TestWriter_AddBlock_on_closed(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	if err = w.AddBlock(TypeWriteAction, []byte("foo"), []byte("bar")); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestWriter_AddBlock_with_invalid_type(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Close file to incur error
	w.f.Close()

	// Set expected error as an invalid type error
	expectedErr := fmt.Errorf("invalid type, <%d> is not supported", 126)

	// Attempt to add block with invalid type
	err = w.AddBlock(126, []byte("foo"), []byte("bar"))

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_AddBlock_with_closed_file(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Close file to incur error
	w.f.Close()

	expectedErr := fmt.Errorf("write %s: file already closed", w.filename)
	err = w.AddBlock(TypeWriteAction, []byte("foo"), []byte("bar"))

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_Merge(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "primary"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.AddBlock(TypeWriteAction, []byte("0"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter("./test_data", "chunk"); err != nil {
		t.Fatal(err)
	}
	defer chunk.Close()

	// Initialize chunk
	chunk.init(w.m, time.Now().UnixNano())

	if err = chunk.AddBlock(TypeWriteAction, []byte("1"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("2"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.SetIndex(2); err != nil {
		t.Fatal(err)
	}

	chunkFilename := chunk.filename
	chunkSize := chunk.m.TotalBlockSize

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	beforeSize := w.m.TotalBlockSize
	expectedTotal := chunkSize + beforeSize

	if err = Read(chunkFilename, func(r *Reader) (err error) {
		return w.Merge(r)
	}); err != nil {
		t.Fatal(err)
	}

	switch {
	case w.m.CurrentIndex != 2:
		t.Fatalf("invalid index, expected %d and received %d", 2, w.m.CurrentIndex)
	case w.m.TotalBlockSize != expectedTotal:
		t.Fatalf("invalid total block size, expected %d bytes and received %d", expectedTotal, w.m.TotalBlockSize)
	}
}

func TestWriter_Merge_stale(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "primary"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.AddBlock(TypeWriteAction, []byte("0"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter("./test_data", "chunk"); err != nil {
		t.Fatal(err)
	}
	defer chunk.Close()

	// Initialize chunk
	chunk.init(w.m, time.Now().UnixNano())

	if err = chunk.AddBlock(TypeWriteAction, []byte("1"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("2"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.SetIndex(2); err != nil {
		t.Fatal(err)
	}

	chunkFilename := chunk.filename
	chunkSize := chunk.m.TotalBlockSize

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	beforeSize := w.m.TotalBlockSize
	expectedTotal := chunkSize + beforeSize

	if err = Read(chunkFilename, func(r *Reader) (err error) {
		if err = w.Merge(r); err != nil {
			return
		}

		// Stale merge, this should ignore
		return w.Merge(r)
	}); err != nil {
		t.Fatal(err)
	}

	switch {
	case w.m.CurrentIndex != 2:
		t.Fatalf("invalid index, expected %d and received %d", 2, w.m.CurrentIndex)
	case w.m.TotalBlockSize != expectedTotal:
		t.Fatalf("invalid total block size, expected %d bytes and received %d", expectedTotal, w.m.TotalBlockSize)
	}
}

func TestWriter_Merge_with_updated_snapshot(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "primary"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.AddBlock(TypeWriteAction, []byte("0"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter("./test_data", "chunk"); err != nil {
		t.Fatal(err)
	}
	defer chunk.Close()

	// Initialize chunk
	chunk.init(w.m, time.Now().UnixNano())

	// Initialize as snapshot
	chunk.initSnapshot()

	if err = chunk.AddBlock(TypeWriteAction, []byte("1"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("2"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.SetIndex(2); err != nil {
		t.Fatal(err)
	}

	chunkFilename := chunk.filename
	chunkSize := chunk.m.TotalBlockSize

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	if err = Read(chunkFilename, func(r *Reader) (err error) {
		if err = w.Merge(r); err != nil {
			return
		}

		// Stale merge, this should ignore
		return w.Merge(r)
	}); err != nil {
		t.Fatal(err)
	}

	switch {
	case w.m.CurrentIndex != 2:
		t.Fatalf("invalid index, expected %d and received %d", 2, w.m.CurrentIndex)
	case w.m.TotalBlockSize != chunkSize:
		t.Fatalf("invalid total block size, expected %d bytes and received %d", chunkSize, w.m.TotalBlockSize)
	}
}

func TestWriter_Merge_with_updated_snapshot_and_error(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "primary"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.AddBlock(TypeWriteAction, []byte("0"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter("./test_data", "chunk"); err != nil {
		t.Fatal(err)
	}
	defer chunk.Close()

	// Initialize chunk
	chunk.init(w.m, time.Now().UnixNano())

	// Initialize as snapshot
	chunk.initSnapshot()

	if err = chunk.AddBlock(TypeWriteAction, []byte("1"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("2"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.SetIndex(2); err != nil {
		t.Fatal(err)
	}

	chunkFilename := chunk.filename

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	// Close file to induce error
	w.f.Close()

	targetErr := fmt.Errorf("truncate %s: file already closed", w.filename)
	err = Read(chunkFilename, func(r *Reader) (err error) {
		if err = w.Merge(r); err != nil {
			return
		}

		// Stale merge, this should ignore
		return w.Merge(r)
	})

	if err = compareErrors(targetErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_Merge_with_reader_error(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "primary"); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err = w.AddBlock(TypeWriteAction, []byte("0"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter("./test_data", "chunk"); err != nil {
		t.Fatal(err)
	}
	defer chunk.Close()

	// Initialize chunk
	chunk.init(w.m, time.Now().UnixNano())

	// Initialize as snapshot
	chunk.initSnapshot()

	if err = chunk.AddBlock(TypeWriteAction, []byte("1"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("2"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.SetIndex(2); err != nil {
		t.Fatal(err)
	}

	chunkFilename := chunk.filename

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	targetErr := fmt.Errorf("error encountered while copying source blocks: error seeking to first block byte: seek %s: file already closed", chunkFilename)
	err = Read(chunkFilename, func(r *Reader) (err error) {
		f, ok := (r.r).(*os.File)
		if !ok {
			return fmt.Errorf("unexpected type, exptected %T and received %T", f, r.r)
		}

		// Close file to induce error
		f.Close()

		return w.Merge(r)
	})

	if err = compareErrors(targetErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_Filename(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	type testcase struct {
		dir  string
		name string

		expectedFilename string
	}

	tcs := []testcase{
		{
			dir:              "./test_data",
			name:             "testie",
			expectedFilename: "test_data/testie.moj",
		},
		{
			dir:              "./test_data",
			name:             "foobar",
			expectedFilename: "test_data/foobar.moj",
		},
	}

	for _, tc := range tcs {
		var w *Writer
		if w, err = NewWriter(tc.dir, tc.name); err != nil {
			t.Fatal(err)
		}

		if filename := w.Filename(); filename != tc.expectedFilename {
			t.Fatalf("invalid filename, expected <%s> and received <%s>", tc.expectedFilename, filename)
		}
	}
}

func TestWriter_setSize_on_uninitialized(t *testing.T) {
	var err error
	w := &Writer{}
	expectedError := fmt.Errorf("error getting file information: %v", os.ErrInvalid)
	if err = compareErrors(expectedError, w.setSize()); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_setSize_with_read_only_file(t *testing.T) {
	var err error
	w := &Writer{}
	if w.f, err = os.OpenFile("testfile.txt", os.O_CREATE|os.O_RDONLY, 0744); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("testfile.txt")

	expectedError := fmt.Errorf("error setting file size to %d: truncate %s: %v", metaSize, "testfile.txt", os.ErrInvalid)
	if err = compareErrors(expectedError, w.setSize()); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_unmapMeta_already_unmapped(t *testing.T) {
	var err error
	w := &Writer{}

	if err = w.unmapMeta(); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_close_already_closed(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
	}

	if err = compareErrors(nil, w.Close()); err != nil {
		t.Fatal(err)
	}

	if err = compareErrors(errors.ErrIsClosed, w.Close()); err != nil {
		t.Fatal(err)
	}
}

func ExampleNewWriter() {
	var err error
	if testWriter, err = NewWriter("./test_data", "testie"); err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleWriter_GetIndex() {
	var (
		index uint64
		err   error
	)

	if index, err = testWriter.GetIndex(); err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Current index:", index)
}

func ExampleWriter_SetIndex() {
	var err error
	if err = testWriter.SetIndex(1337); err != nil {
		log.Fatal(err)
	}
}

func ExampleWriter_NextIndex() {
	var (
		index uint64
		err   error
	)

	if index, err = testWriter.NextIndex(); err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Next index:", index)
}

func ExampleWriter_AddBlock() {
	var err error
	if err = testWriter.AddBlock(TypeWriteAction, []byte("greeting"), []byte("Hello world!")); err != nil {
		log.Fatalf("error adding row: %v", err)
		return
	}
}

func testSetIndexGetIndex(t *testing.T) {
	var (
		w   *Writer
		err error
	)

	tcs := readerTestcases
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if w, err = NewWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	for _, tc := range tcs {
		if err = w.SetIndex(tc.index); err != nil {
			t.Fatal(err)
		}

		var index uint64
		index, err = w.GetIndex()
		switch {
		case err != nil:
			t.Fatal(err)
		case index != tc.index:
			t.Fatalf("invalid index, expected %d and received %d", tc.index, index)
		}
	}
}

func compareErrors(expected, received error) (err error) {
	aStr := errToString(expected)
	bStr := errToString(received)
	if aStr == bStr {
		return
	}

	return fmt.Errorf("invalid error, expected <%s> and received <%s>", aStr, bStr)
}

func errToString(err error) (out string) {
	switch err {
	case nil:
		return "nil"
	default:
		return err.Error()
	}
}
