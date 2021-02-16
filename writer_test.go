package kiroku

import (
	"os"
	"testing"
)

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

	for i := int64(0); i < 100; i++ {
		if index := w.NextIndex(); index != i {
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
		w.SetIndex(tc.index)
		if index := w.GetIndex(); index != tc.index {
			t.Fatalf("invalid index, expected %d and received %d", tc.index, index)
		}
	}
}
