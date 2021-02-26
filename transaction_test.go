package kiroku

import (
	"fmt"
	"os"
	"testing"
)

func TestTransaction_SetIndex(t *testing.T) {
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

	if err = k.Transaction(func(t *Transaction) (err error) {
		return t.SetIndex(1337)
	}); err != nil {
		t.Fatal(err)
		return
	}

	var m Meta
	if m, err = k.Meta(); err != nil {
		t.Fatal(err)
	}

	if m.CurrentIndex != 1337 {
		t.Fatalf("invalid index, expected %d and received %d", 1337, m.CurrentIndex)
	}
}

func TestTransaction_GetIndex(t *testing.T) {
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

	if err = k.Transaction(func(t *Transaction) (err error) {
		if err = t.SetIndex(1337); err != nil {
			return
		}

		var index uint64
		if index, err = t.GetIndex(); err != nil {
			return
		}

		if index != 1337 {
			return fmt.Errorf("invalid index, expected %d and received %d", 1337, index)
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}
}

func TestTransaction_NextIndex(t *testing.T) {
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

	if err = k.Transaction(func(t *Transaction) (err error) {
		for i := uint64(0); i < 100; i++ {
			var index uint64
			if index, err = t.NextIndex(); err != nil {
				return
			}

			if index != i {
				return fmt.Errorf("invalid index, expected %d and received %d", i, index)
			}
		}

		return
	}); err != nil {
		t.Fatal(err)
		return
	}
}

func TestTransaction_AddBlock(t *testing.T) {
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
