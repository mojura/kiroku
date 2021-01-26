package history

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestHistory_Transaction(t *testing.T) {
	var (
		h   *History
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if h, err = New("./test_data", "tester", func(m *Meta, r io.ReadSeeker) (err error) {
		buf := bytes.NewBuffer(nil)
		if _, err = io.Copy(buf, r); err != nil {
			return
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}
	defer h.Close()

	if err = h.Transaction(func(c *Chunk) (err error) {
		c.SetIndex(1337)
		c.AddRow(TypeWriteAction, []byte("hello world!"))
		return
	}); err != nil {
		t.Fatal(err)
		return
	}

	time.Sleep(time.Second * 5)
}
