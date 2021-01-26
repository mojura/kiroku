package history

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestKiroku_Transaction(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("./test_data", "tester", func(m *Meta, r io.ReadSeeker) (err error) {
		buf := bytes.NewBuffer(nil)
		if _, err = io.Copy(buf, r); err != nil {
			return
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}
	defer k.Close()

	if err = k.Transaction(func(w *Writer) (err error) {
		w.SetIndex(1337)
		w.AddRow(TypeWriteAction, []byte("hello world!"))
		return
	}); err != nil {
		t.Fatal(err)
		return
	}

	time.Sleep(time.Second * 5)
}
