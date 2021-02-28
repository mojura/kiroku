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
