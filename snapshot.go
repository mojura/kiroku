package kiroku

func newSnapshot(w *Writer) (sp *Snapshot, err error) {
	if err = w.initSnapshot(); err != nil {
		return
	}

	var s Snapshot
	s.w = w
	sp = &s
	return
}

// Snapshot manages a Kiroku transaction
type Snapshot struct {
	w *Writer
}

// Write will add a write block to a writer
func (s *Snapshot) Write(key, value []byte) (err error) {
	return s.w.AddBlock(TypeWriteAction, key, value)
}
