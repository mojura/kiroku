package kiroku

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/hatchify/errors"
)

var testMirror *Mirror

func TestNewMirror(t *testing.T) {
	var (
		m   *Mirror
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
		src  Source

		expectedError error
	}

	tcs := []testcase{
		{
			dir:           "test_data",
			name:          "tester",
			src:           newErrorSource(io.EOF),
			expectedError: nil,
		},
		{
			dir:           "invalid_dir",
			name:          "tester",
			src:           newErrorSource(io.EOF),
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: no such file or directory`, "invalid_dir/tester.moj"),
		},
		{
			dir:           "test_data",
			name:          "invalid_perms",
			src:           newErrorSource(io.EOF),
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: permission denied`, "test_data/invalid_perms.moj"),
		},
		{
			dir:           "test_data",
			name:          "tester",
			src:           newErrorSource(errors.New("foobar")),
			expectedError: fmt.Errorf("error getting last file: %s", "foobar"),
		},
	}

	for _, tc := range tcs {
		opts := MakeOptions(tc.dir, tc.name)
		m, err = NewMirror(opts, tc.src)
		if err = compareErrors(tc.expectedError, err); err != nil {
			t.Fatal(err)
		}

		if m == nil {
			continue
		}

		// Sleep to allow time for scan to run
		time.Sleep(time.Millisecond * 100)

		if err = m.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMirror_Filename(t *testing.T) {
	var (
		m   *Mirror
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")
	opts := MakeOptions("test_data", "tester")
	if m, err = NewMirror(opts, newErrorSource(io.EOF)); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	var filename string
	if filename, err = m.Filename(); err != nil {
		t.Fatal(err)
	}

	if filename != "test_data/tester.moj" {
		t.Fatalf("invalid filename, expected <%s and received <%s>", "test_data/tester.moj", filename)
	}
}

func TestMirror_Meta(t *testing.T) {
	var (
		m   *Mirror
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")
	opts := MakeOptions("test_data", "tester")
	if m, err = NewMirror(opts, newErrorSource(io.EOF)); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err = m.k.Transaction(func(t *Transaction) (err error) {
		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	var meta Meta
	meta, err = m.Meta()
	switch {
	case err != nil:
	case meta.isEmpty():
		t.Fatal("found empty meta when non empty was expected")
		return
	}
}

func TestMirror_Close(t *testing.T) {
	var (
		m   *Mirror
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	opts := MakeOptions("test_data", "test")
	if m, err = NewMirror(opts, newErrorSource(io.EOF)); err != nil {
		t.Fatal(err)
	}

	type testcase struct {
		expectedError error
	}

	tcs := []testcase{
		{
			expectedError: nil,
		},
		{
			expectedError: errors.ErrIsClosed,
		},
	}

	for _, tc := range tcs {
		if err = compareErrors(tc.expectedError, m.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

func ExampleNewMirror() {
	var err error
	opts := MakeOptions("./test_data", "tester")
	if testMirror, err = NewMirror(opts, newErrorSource(io.EOF)); err != nil {
		log.Fatal(err)
		return
	}
}
