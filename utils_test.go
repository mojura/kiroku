package kiroku

import (
	"fmt"
	"os"
	"testing"
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
