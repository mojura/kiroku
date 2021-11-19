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
func (t *Transaction) AddBlock(typ Type, key, value []byte) (err error) {
	return t.w.AddBlock(typ, key, value)
}

// AddBlock will add a row
func (t *Transaction) Meta() (m Meta) {
	return t.w.Meta()
}
