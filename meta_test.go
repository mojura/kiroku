package kiroku

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func Test_newMetaFromReader_with_error(t *testing.T) {
	var (
		f   *os.File
		err error
	)

	type testcase struct {
		r      io.ReadSeeker
		errStr string
	}

	tcs := []testcase{
		{
			r:      f,
			errStr: "error encountered while seeking to beginning of file: invalid argument",
		},
		{
			r:      bytes.NewReader(make([]byte, 12)),
			errStr: "error reading meta bytes: unexpected EOF",
		},
	}

	for _, tc := range tcs {
		var errStr string
		if _, err = newMetaFromReader(tc.r); err != nil {
			errStr = err.Error()
		}

		if errStr != tc.errStr {
			t.Fatalf("invalid error, expected <%s>, received <%v>", tc.errStr, err)
		}
	}
}

func TestMeta_merge_nil(t *testing.T) {
	var m Meta
	m.merge(nil)
}
