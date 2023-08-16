package kiroku

func newTransaction(w *Writer) *Transaction {
	var t Transaction
	t.w = w
	return &t
}

// Transaction manages a Kiroku transaction
type Transaction struct {
	w *Writer
}

// AddBlock will add a row
func (t *Transaction) Write(value []byte) (err error) {
	return t.w.Write(value)
}

type TransactionFn func(*Transaction) error
