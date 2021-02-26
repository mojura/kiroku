package kiroku

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
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
	if err = k.Close(); err != nil {
		t.Fatal(err)
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
		if err = s.Write([]byte("1"), []byte("hello world!")); err != nil {
			return
		}

		return
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
