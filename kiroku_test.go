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

	if k, err = New("test_data", "tester", nil); err != nil {
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

	if k, err = New("test_data", "tester", nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(w *Writer) (err error) {
		if err = w.SetIndex(1337); err != nil {
			return
		}

		return w.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
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

	if k, err = New("test_data", "tester", nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(w *Writer) (err error) {
		if err = w.SetIndex(1337); err != nil {
			return
		}

		return w.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if k, err = New("test_data", "tester", nil); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Transaction(func(w *Writer) (err error) {
		var index uint64
		index, err = w.GetIndex()
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

	if k, err = New("./test_data", "tester", pfn); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if err = k.Transaction(func(w *Writer) (err error) {
		if err = w.SetIndex(1337); err != nil {
			return
		}

		return w.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Close(); err != nil {
		t.Fatal(err)
	}

	if k, err = New("test_data", "tester", pfn); err != nil {
		t.Fatal(err)
		return
	}

	if err = k.Transaction(func(w *Writer) (err error) {
		var index uint64
		index, err = w.GetIndex()
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

func ExampleNew() {
	var err error
	if testKiroku, err = New("./test_data", "tester", nil); err != nil {
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

	if testKiroku, err = New("./test_data", "tester", pfn); err != nil {
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
	if err = testKiroku.Transaction(func(w *Writer) (err error) {
		if err = w.SetIndex(1337); err != nil {
			return
		}

		return w.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		log.Fatal(err)
		return
	}
}
