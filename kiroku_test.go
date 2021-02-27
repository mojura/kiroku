package kiroku

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

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
	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()
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
				AvoidMergeOnInit:    false,
				AvoidProcessOnInit:  false,
				AvoidMergeOnClose:   false,
				AvoidProcessOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:    true,
				AvoidProcessOnInit:  false,
				AvoidMergeOnClose:   false,
				AvoidProcessOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:    true,
				AvoidProcessOnInit:  true,
				AvoidMergeOnClose:   false,
				AvoidProcessOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:    true,
				AvoidProcessOnInit:  true,
				AvoidMergeOnClose:   true,
				AvoidProcessOnClose: false,
			},
		},
		{
			options: &Options{
				AvoidMergeOnInit:    true,
				AvoidProcessOnInit:  true,
				AvoidMergeOnClose:   true,
				AvoidProcessOnClose: true,
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

func TestKiroku_Transaction_with_standard_processor(t *testing.T) {
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
	pfn := func(r *Reader) (err error) {
		wg.Done()
		return
	}

	if k, err = New("./test_data", "tester", pfn, nil); err != nil {
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

	if k, err = New("test_data", "tester", pfn, nil); err != nil {
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

func ExampleNew() {
	var err error
	if testKiroku, err = New("./test_data", "tester", nil, nil); err != nil {
		log.Fatal(err)
		return
	}
}

func ExampleNew_with_custom_Processor() {
	var err error
	pfn := func(r *Reader) (err error) {
		fmt.Println("Hello chunk!", r.Meta())
		return
	}

	if testKiroku, err = New("./test_data", "tester", pfn, nil); err != nil {
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
