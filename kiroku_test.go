package kiroku

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hatchify/errors"
)

var testKiroku *Kiroku

func TestNew(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var invalidPerms *os.File
	if invalidPerms, err = os.OpenFile("./test_data/invalid_perms.moj", os.O_CREATE|os.O_RDWR, 0511); err != nil {
		t.Fatal(err)
	}

	if err = invalidPerms.Close(); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		dir  string
		name string

		expectedError error
	}

	tcs := []testcase{
		{
			dir:           "test_data",
			name:          "tester",
			expectedError: nil,
		},
		{
			dir:           "invalid_dir",
			name:          "tester",
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: no such file or directory`, "invalid_dir/tester.moj"),
		},
		{
			dir:           "test_data",
			name:          "invalid_perms",
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: permission denied`, "test_data/invalid_perms.moj"),
		},
	}

	for _, tc := range tcs {
		k, err = New(tc.dir, tc.name, nil, nil)
		if err = compareErrors(tc.expectedError, err); err != nil {
			t.Fatal(err)
		}

		if k == nil {
			continue
		}

		if err = k.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNew_with_loading_unmerged_chunk(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var opts Options
	opts.AvoidMergeOnInit = true
	opts.AvoidMergeOnClose = true

	if k, err = New("test_data", "test", nil, &opts); err != nil {
		t.Fatal(err)
	}

	if err = k.Transaction(func(txn *Transaction) (err error) {
		// Set index to 100 to add an extra potential for failure on unprocessed chunk importing
		if err = txn.SetIndex(100); err != nil {
			return
		}

		var index uint64
		// Create 10 entries
		for i := 0; i < 10; i++ {
			if index, err = txn.NextIndex(); err != nil {
				return
			}

			indexStr := strconv.FormatUint(index, 10)

			if err = txn.AddBlock(TypeWriteAction, []byte(indexStr), []byte("value")); err != nil {
				return
			}
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = k.Close(); err != nil {
		return
	}

	if k, err = New("test_data", "test", nil, &opts); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	var m Meta
	if m, err = k.Meta(); err != nil {
		t.Fatal(err)
	}

	switch {
	case m.CurrentIndex != 110:
		t.Fatalf("invalid current index, expected %d and received %d", 110, m.CurrentIndex)
	case m.BlockCount != 10:
		t.Fatalf("invalid block count, expected %d and received %d", 10, m.BlockCount)
	}
}

func TestNew_with_invalid_merging_chunk_permissions(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.OpenFile("./test_data/test.chunk.moj", os.O_CREATE|os.O_RDWR, 0111); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("error encountered while merging: open %s: permission denied", "test_data/test.chunk.moj")

	if k, err = New("test_data", "test", nil, nil); k != nil {
		defer k.Close()
	}

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestNew_with_invalid_exporting_chunk_permissions(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.OpenFile("./test_data/test.merged.moj", os.O_CREATE|os.O_RDWR, 0111); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("error encountered while exporting: open %s: permission denied", "test_data/test.merged.moj")
	exp := &testExporter{}

	if k, err = New("test_data", "test", exp, nil); err != nil {
		t.Fatal(err)
	}

	if err = compareErrors(expectedErr, k.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestNew_with_error_initing_meta(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var opts Options
	opts.AvoidMergeOnInit = true

	var f *os.File
	if f, err = os.OpenFile("./test_data/test.chunk.moj", os.O_CREATE|os.O_RDWR, 0111); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("error initializing meta: open %s: permission denied", "test_data/test.chunk.moj")

	if k, err = New("test_data", "test", nil, &opts); k != nil {
		defer k.Close()
	}

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}
}

func TestNew_with_options(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	type testcase struct {
		options *Options
		err     error
	}

	tcs := []testcase{
		{
			options: nil,
		},
		{
			options: &Options{
				AvoidMergeOnInit:   false,
				AvoidMergeOnClose:  false,
				AvoidExportOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:   true,
				AvoidMergeOnClose:  false,
				AvoidExportOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:   true,
				AvoidMergeOnClose:  false,
				AvoidExportOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:   true,
				AvoidMergeOnClose:  true,
				AvoidExportOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:   true,
				AvoidMergeOnClose:  true,
				AvoidExportOnClose: true,
			},
		},
	}

	fn := func(tc testcase) (err error) {
		if err = os.Mkdir("test_data", 0744); err != nil {
			return
		}
		defer os.RemoveAll("./test_data")

		if k, err = New("test_data", "tester", nil, tc.options); err != tc.err {
			return fmt.Errorf("invalid error, expected <%v> and received <%v>", tc.err, err)
		}

		if err != nil {
			return
		}

		return k.Close()
	}

	for _, tc := range tcs {
		if err = fn(tc); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKiroku_initMeta_with_error(t *testing.T) {
	var k Kiroku
	k.dir = "test_data"
	expectedErr := fmt.Errorf("lstat %s: no such file or directory", "test_data")
	if err := compareErrors(expectedErr, k.initMeta()); err != nil {
		t.Fatal(err)
	}
}

func TestKiroku_Filename(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")
	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	var filename string
	if filename, err = k.Filename(); err != nil {
		t.Fatal(err)
	}

	if filename != "test_data/tester.moj" {
		t.Fatalf("invalid filename, expected <%s and received <%s>", "test_data/tester.moj", filename)
	}
}

func TestKiroku_Filename_on_closed(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")
	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = k.Filename(); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestKiroku_Meta(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	var m Meta
	m, err = k.Meta()
	switch {
	case err != nil:
		return
	case m.CurrentIndex != 1337:
		t.Fatalf("invalid index, expected %d and received %d", 1337, m.CurrentIndex)
		return
	}
}

func TestKiroku_Meta_on_closed(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	k.Close()

	if _, err = k.Meta(); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
	}
}

func TestKiroku_Transaction_with_nil_exporter(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Transaction(func(t *Transaction) (err error) {
		var index uint64
		index, err = t.GetIndex()
		switch {
		case err != nil:
			return
		case index != 1337:
			err = fmt.Errorf("invalid index, expected %d and received %d", 1337, index)
			return
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}

	k.Close()
}

func TestKiroku_Transaction_with_custom_processor(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var wg sync.WaitGroup
	wg.Add(2)

	exp := &testExporter{
		wg: &wg,
	}

	if k, err = New("./test_data", "tester", exp, nil); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
	}

	if k, err = New("test_data", "tester", exp, nil); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Transaction(func(t *Transaction) (err error) {
		var index uint64
		index, err = t.GetIndex()
		switch {
		case err != nil:
			return
		case index != 1337:
			err = fmt.Errorf("invalid index, expected %d and received %d", 1337, index)
			return
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}

	wg.Wait()
	k.Close()
}

func TestKiroku_Transaction_on_closed(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
	}

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
		return
	}
}

func TestKiroku_Snapshot(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		if err = t.AddBlock(TypeWriteAction, []byte("0"), []byte("hello world!")); err != nil {
			return
		}

		if err = t.AddBlock(TypeDeleteAction, []byte("0"), []byte("hello world!")); err != nil {
			return
		}

		if err = t.AddBlock(TypeWriteAction, []byte("1"), []byte("hello world!")); err != nil {
			return
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Snapshot(func(s *Snapshot) (err error) {
		return s.Write([]byte("1"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	var m Meta
	if m, err = k.Meta(); err != nil {
		t.Fatal(err)
	}

	switch {
	case m.BlockCount != 1:
		t.Fatalf("invalid block count, expected %d and received %d", 1, m.BlockCount)
	}
}

func TestKiroku_Snapshot_on_closed(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
	}

	if err = k.Snapshot(func(s *Snapshot) (err error) {
		return s.Write([]byte("1"), []byte("hello world!"))
	}); err != errors.ErrIsClosed {
		t.Fatalf("invalid error, expected <%v> and received <%v>", errors.ErrIsClosed, err)
		return
	}
}

func TestKiroku_Snapshot_with_error(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	targetErr := errors.Error("foobar")
	if err = k.Snapshot(func(s *Snapshot) (err error) {
		return targetErr
	}); err != targetErr {
		t.Fatalf("invalid error, expected <%v> and received <%v>", targetErr, err)
		return
	}
}

func TestKiroku_rename_with_invalid_permissions(t *testing.T) {
	var err error
	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.OpenFile("./test_data/test.chunk.moj", os.O_CREATE|os.O_RDWR, 0600); err != nil {
		t.Fatal(err)
	}

	if err = os.Chmod("test_data", 0600); err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	var k Kiroku
	k.name = "test"
	k.dir = "test_data"

	unix := time.Now().UnixNano()
	expectedErr := fmt.Errorf("rename %s test_data/test.merged.%d.moj: permission denied", "test_data/test.chunk.moj", unix)
	err = k.rename("test_data/test.chunk.moj", "merged", unix)

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}

	if err = os.Chmod("test_data", 0744); err != nil {
		t.Fatal(err)
	}
}

func TestKiroku_exportAndRemove_with_invalid_permissions(t *testing.T) {
	var err error
	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var k Kiroku
	k.name = "test"
	k.dir = "test_data"

	if k.c, err = NewWriter(k.dir, k.name); err != nil {
		t.Fatal(err)
	}

	var chunk *Writer
	if chunk, err = NewWriter(k.dir, k.name+".chunk"); err != nil {
		t.Fatal(err)
	}

	if err = chunk.AddBlock(TypeWriteAction, []byte("foo"), []byte("bar")); err != nil {
		t.Fatal(err)
	}

	if err = chunk.Close(); err != nil {
		t.Fatal(err)
	}

	if err = os.Chmod("test_data", 0600); err != nil {
		t.Fatal(err)
	}

	expectedErr := fmt.Errorf("remove %s: permission denied", "test_data/test.chunk.moj")
	err = k.exportAndRemove(chunk.filename)

	if err = compareErrors(expectedErr, err); err != nil {
		t.Fatal(err)
	}

	if err = os.Chmod("test_data", 0744); err != nil {
		t.Fatal(err)
	}
}

func TestKiroku_sleep(t *testing.T) {
	type testcase struct {
		ctx      context.Context
		duration time.Duration
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tcs := []testcase{
		{
			ctx:      context.Background(),
			duration: time.Millisecond * 100,
		},
		{
			ctx:      cancelled,
			duration: time.Millisecond * 100,
		},
	}

	for _, tc := range tcs {
		var k Kiroku
		k.ctx = tc.ctx
		k.sleep(tc.duration)
	}

}

func ExampleNew() {
	var err error
	if testKiroku, err = New("./test_data", "tester", nil, nil); err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleNew_with_custom_Exporter() {
	var (
		e   Exporter
		err error
	)

	// Utilize any Exporter, see https://github.com/mojura/sync-s3 for an example

	if testKiroku, err = New("./test_data", "tester", e, nil); err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleKiroku_Meta() {
	var (
		m   Meta
		err error
	)

	if m, err = testKiroku.Meta(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Meta", m)
}

func ExampleKiroku_Transaction() {
	var err error
	if err = testKiroku.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleKiroku_Snapshot() {
	var err error
	if err = testKiroku.Snapshot(func(s *Snapshot) (err error) {
		return s.Write([]byte("testKey"), []byte("hello world!"))
	}); err != nil {
		log.Fatal(err)
		return
	}
}

type testExporter struct {
	wg *sync.WaitGroup
}

func (e *testExporter) Export(filename string, r io.Reader) (err error) {
	if e.wg == nil {
		return
	}

	e.wg.Done()
	return
}
