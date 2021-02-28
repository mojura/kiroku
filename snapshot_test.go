package kiroku

import (
	"os"
	"testing"
)

func TestSnapshot_Write(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if k, err = New("test_data", "tester", nil, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Snapshot(func(t *Snapshot) (err error) {
		if err = t.Write([]byte("0"), []byte("test!")); err != nil {
			return
		}

		if err = t.Write([]byte("1"), []byte("test!")); err != nil {
			return
		}

		if err = t.Write([]byte("2"), []byte("test!")); err != nil {
			return
		}

		return t.Write([]byte("3"), []byte("test!"))
	}); err != nil {
		t.Fatal(err)
		return
	}

	var m Meta
	if m, err = k.Meta(); err != nil {
		t.Fatal(err)
	}

	if m.BlockCount != 4 {
		t.Fatalf("invalid block count, expected %d and received %d", 4, m.BlockCount)
	}
}
