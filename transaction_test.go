package kiroku

import (
	"os"
	"testing"
)

func TestTransaction_AddBlock(t *testing.T) {
	var (
		k   *Kiroku
		err error
	)

	if err = os.Mkdir("test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	opts := MakeOptions("test_data", "tester")
	if k, err = New(opts, nil); err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.AddBlock(TypeWriteAction, []byte("0"), []byte("test!")); err != nil {
			return
		}

		if err = t.AddBlock(TypeWriteAction, []byte("1"), []byte("test!")); err != nil {
			return
		}

		if err = t.AddBlock(TypeWriteAction, []byte("2"), []byte("test!")); err != nil {
			return
		}

		return t.AddBlock(TypeWriteAction, []byte("3"), []byte("test!"))
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
