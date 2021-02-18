package kiroku

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var testWriter *Writer

func Test_newWriter(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if _, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}
}

func TestWriter_GetIndex(t *testing.T) {
	testSetIndexGetIndex(t)
}

func TestWriter_SetIndex(t *testing.T) {
	testSetIndexGetIndex(t)
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

	if w, err = newWriter("./test_data", "testie"); err != nil {
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

func TestWriter_AddRow(t *testing.T) {
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

	if w, err = newWriter("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	for _, tc := range tcs {
		if err = w.AddRow(tc.t, []byte(tc.data)); err != nil {
			t.Fatalf("error adding row: %v", err)
		}
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

func ExampleWriter_AddRow() {
	var err error
	if err = testWriter.AddRow(TypeWriteAction, []byte("Hello world!")); err != nil {
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

	if w, err = newWriter("./test_data", "testie"); err != nil {
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
