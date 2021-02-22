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
		w.SetIndex(1337)
		w.AddBlock(TypeWriteAction, []byte("hello world!"))
		return
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
		w.SetIndex(1337)
		w.AddBlock(TypeWriteAction, []byte("hello world!"))
		return
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

func ExampleKiroku_Transaction() {
	var err error
	if err = testKiroku.Transaction(func(w *Writer) (err error) {
		w.SetIndex(1337)
		w.AddBlock(TypeWriteAction, []byte("hello world!"))
		return
	}); err != nil {
		log.Fatal(err)
		return
	}
}
