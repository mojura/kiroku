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

// GetIndex will get the current index value
func (t *Transaction) GetIndex() (index uint64, err error) {
	return t.w.GetIndex()
}

// NextIndex will get the current index value then increment the internal value
func (t *Transaction) NextIndex() (index uint64, err error) {
	return t.w.NextIndex()
}

// SetIndex will set the index value
// Note: This can be used to manually set an index to a desired value
func (t *Transaction) SetIndex(index uint64) (err error) {
	return t.w.SetIndex(index)
}

// AddBlock will add a row
func (t *Transaction) AddBlock(typ Type, key, value []byte) (err error) {
	return t.w.AddBlock(typ, key, value)
}
