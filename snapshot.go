package kiroku

func newSnapshot(w *Writer) *Snapshot {
	var s Snapshot
	s.w = w
	return &s
}

// Snapshot manages a Kiroku transaction
type Snapshot struct {
	w *Writer
}

// Write will add a write block to a writer
func (s *Snapshot) Write(key, value []byte) (err error) {
	return s.w.AddBlock(TypeWriteAction, key, value)
}
