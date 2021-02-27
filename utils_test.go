package kiroku

import (
	"io"
	"os"
	"testing"
)

func Test_walkFn(t *testing.T) {
	fn := walkFn(func(filename string, info os.FileInfo) (err error) {
		return
	})

	if err := fn("filename.txt", nil, io.EOF); err != nil {
		t.Fatal(err)
	}
}
