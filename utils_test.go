package kiroku

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/hatchify/errors"
)

func Test_walk(t *testing.T) {
	expectedErr := fmt.Errorf("lstat %s: no such file or directory", "invalid_dir")
	fn := func(filename string, info os.FileInfo) (err error) {
		return
	}

	if err := compareErrors(expectedErr, walk("invalid_dir", fn)); err != nil {
		t.Fatal(err)
	}
}

func Test_walk_files_removed_mid_iteration(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	for i := 0; i < 100; i++ {
		var f *os.File
		filename := fmt.Sprintf("./test_data/%d.txt", i)
		if f, err = os.Create(filename); err != nil {
			t.Fatal(err)
		}

		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	fn := func(filename string, info os.FileInfo) (err error) {
		return
	}

	errC := make(chan error)
	go func() {
		for i := 99; i > -1; i-- {
			filename := fmt.Sprintf("./test_data/%d.txt", i)
			if err = os.Remove(filename); err != nil {
				errC <- err
				return
			}
		}

		errC <- nil
		close(errC)
	}()

	for i := 0; i < 10; i++ {
		if err := compareErrors(nil, walk("test_data", fn)); err != nil {
			t.Fatal(err)
		}
	}

	if err = <-errC; err != nil {
		t.Fatal(err)
	}
}

func Test_removeFile(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.Create("./test_data/test"); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err = removeFile(f, "./test_data"); err != nil {
		t.Fatal(err)
	}

	if err = removeFile(f, "./test_data"); err == nil {
		t.Fatal("expected error and received nil")
	}
}

func Test_removeFile_with_close_error(t *testing.T) {
	var err error
	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	var f *os.File
	if f, err = os.Create("./test_data/test"); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	mf := mockFile{
		stat: func() (fs.FileInfo, error) {
			return f.Stat()
		},
		read: func(bs []byte) (int, error) {
			return f.Read(bs)
		},
		close: func() error { return errors.New("foobar") },
	}

	err = removeFile(&mf, "./test_data")
	switch {
	case err == nil:
		t.Fatalf("invalid error, expected <foobar> and received nil")
	case err.Error() != "foobar":
		t.Fatalf("invalid error, expected <%s> and received <%s>", "foobar", err.Error())
	}
}

func Test_isNilSrc(t *testing.T) {
	type testcase struct {
		getSrc  func() Source
		expects bool
	}

	tcs := []testcase{
		{
			getSrc:  func() Source { return nil },
			expects: true,
		},
		{
			getSrc: func() Source {
				var s Source
				return s
			},
			expects: true,
		},
		{
			getSrc:  func() Source { return &mockSource{} },
			expects: false,
		},
	}

	for i, tc := range tcs {
		val := isNilSource(tc.getSrc())
		if val != tc.expects {
			t.Fatalf("invalid value, expected %v and received %v (Test case #%d)", tc.expects, val, i)
		}
	}
}

type mockFile struct {
	stat  func() (fs.FileInfo, error)
	read  func([]byte) (int, error)
	close func() error
}

func (m *mockFile) Stat() (fs.FileInfo, error) { return m.stat() }

func (m *mockFile) Read(bs []byte) (int, error) { return m.read(bs) }

func (m *mockFile) Close() error { return m.close() }
