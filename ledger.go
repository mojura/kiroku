package kiroku

type Ledger interface {
	Meta() (m Meta, err error)
	Transaction(fn func(*Transaction) error) (err error)
	Snapshot(fn func(*Snapshot) error) (err error)
	Filename() (filename string, err error)
	Close() (err error)
}
