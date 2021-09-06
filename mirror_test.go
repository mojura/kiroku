package kiroku

import (
	"context"
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
		dir      string
		name     string
		importer Importer

		expectedError error
	}

	tcs := []testcase{
		{
			dir:           "test_data",
			name:          "tester",
			importer:      newErrorImporter(io.EOF),
			expectedError: nil,
		},
		{
			dir:           "invalid_dir",
			name:          "tester",
			importer:      newErrorImporter(io.EOF),
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: no such file or directory`, "invalid_dir/tester.moj"),
		},
		{
			dir:           "test_data",
			name:          "invalid_perms",
			importer:      newErrorImporter(io.EOF),
			expectedError: fmt.Errorf(`error initializing primary chunk: open %s: permission denied`, "test_data/invalid_perms.moj"),
		},
		{
			dir:           "test_data",
			name:          "tester",
			importer:      newErrorImporter(errors.New("foobar")),
			expectedError: nil,
		},
	}

	for _, tc := range tcs {
		opts := MakeMirrorOptions(tc.dir, tc.name, nil)
		m, err = NewMirror(opts, tc.importer)
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
	opts := MakeMirrorOptions("test_data", "tester", nil)
	if m, err = NewMirror(opts, newErrorImporter(io.EOF)); err != nil {
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
	opts := MakeMirrorOptions("test_data", "tester", nil)
	if m, err = NewMirror(opts, newErrorImporter(io.EOF)); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err = m.k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("testKey"), []byte("hello world!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	var meta Meta
	meta, err = m.Meta()
	switch {
	case err != nil:
		return
	case meta.CurrentIndex != 1337:
		t.Fatalf("invalid index, expected %d and received %d", 1337, meta.CurrentIndex)
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

	opts := MakeMirrorOptions("test_data", "test", nil)
	if m, err = NewMirror(opts, newErrorImporter(io.EOF)); err != nil {
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
	opts := MakeMirrorOptions("./test_data", "tester", nil)
	if testMirror, err = NewMirror(opts, newErrorImporter(io.EOF)); err != nil {
		log.Fatal(err)
		return
	}
}

func newErrorImporter(err error) *errorImporter {
	var e errorImporter
	e.err = err
	return &e
}

type errorImporter struct {
	err error
}

func (e *errorImporter) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	err = e.err
	return
}

func (e *errorImporter) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	err = e.err
	return
}

func newMockImporter(g getNextFn, i importFn) *mockImporter {
	var m mockImporter
	m.getNextFn = g
	m.importFn = i
	return &m
}

type mockImporter struct {
	getNextFn getNextFn
	importFn  importFn
}

func (m *mockImporter) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	return m.getNextFn(ctx, prefix, lastFilename)
}

func (m *mockImporter) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	return m.importFn(ctx, filename, w)
}

type getNextFn func(ctx context.Context, prefix, lastFilename string) (filename string, err error)
type importFn func(ctx context.Context, filename string, w io.Writer) (err error)
