package history

import (
	"fmt"
	"os"
	"testing"
)

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
		w.AddRow(TypeWriteAction, []byte("hello world!"))
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
		if index := w.GetIndex(); index != 1337 {
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

	pfn := func(r *Reader) (err error) {
		fmt.Println("Meta!", r.Meta())
		return
	}

	if k, err = New("./test_data", "tester", pfn); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if err = k.Transaction(func(w *Writer) (err error) {
		w.SetIndex(1337)
		w.AddRow(TypeWriteAction, []byte("hello world!"))
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
		if index := w.GetIndex(); index != 1337 {
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
