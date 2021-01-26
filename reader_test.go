package history

import (
	"fmt"
	"os"
	"testing"
)

func TestReader_ForEach(t *testing.T) {
	var (
		c   *Chunk
		err error
	)

	type testcase struct {
		t     Type
		data  string
		index int64
	}

	tcs := []testcase{
		{
			t:     TypeWriteAction,
			data:  "foo 1",
			index: 1,
		},
		{
			t:     TypeWriteAction,
			data:  "foo 2",
			index: 2,
		},
		{
			t:     TypeWriteAction,
			data:  "foo 3",
			index: 3,
		},
		{
			t:     TypeDeleteAction,
			data:  "",
			index: 3,
		},
	}

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll("./test_data")

	if c, err = newChunk("./test_data", "testie"); err != nil {
		t.Fatal(err)
		return
	}

	for _, tc := range tcs {
		if err = c.AddRow(tc.t, []byte(tc.data)); err != nil {
			t.Fatal(err)
		}

		c.SetIndex(tc.index)
	}

	if _, err = c.f.Seek(0, 0); err != nil {
		t.Fatalf("error setting file to start: %v", err)
		return
	}

	var r *Reader
	if r, err = NewReader(c.f); err != nil {
		t.Fatalf("error initializing reader: %v", err)
		return
	}

	var count int
	if err = r.ForEach(func(b *Block) (err error) {
		tc := tcs[count]
		if str := string(b.Data); str != tc.data {
			err = fmt.Errorf("invalid data, expected <%s> and received <%s>", tc.data, str)
			return
		}

		count++
		return
	}); err != nil {
		t.Fatalf("error during iteration: %v", err)
		return
	}

	if count != len(tcs) {
		t.Fatalf("invalid number of iterations, expected %d and received %d", len(tcs), count)
	}

	meta := r.Meta()
	if last := tcs[len(tcs)-1]; meta.CurrentIndex != last.index {
		t.Fatalf("invalid index, expected %d and received %d", last.index, meta.CurrentIndex)
	}

	if meta.RowCount != int64(len(tcs)) {
		t.Fatalf("invalid row count, expected %d and received %d", len(tcs), meta.RowCount)
	}
}
