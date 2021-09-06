package kiroku

import "github.com/hatchify/errors"

const (
	// ErrEmptyDirectory is returned when a directory is empty
	ErrEmptyDirectory = errors.Error("invalid directory, cannot be empty")
	// ErrEmptyName is returned when a name is empty
	ErrEmptyName = errors.Error("invalid name, cannot be empty")
)

// MakeOptions will create new Options
func MakeOptions(dir, name string) (o Options) {
	o.Dir = dir
	o.Name = name
	return
}

// Options represent Kiroku options
type Options struct {
	Dir  string
	Name string

	AvoidMergeOnInit   bool
	AvoidMergeOnClose  bool
	AvoidExportOnClose bool
}

func (o *Options) Validate() (err error) {
	var errs errors.ErrorList
	if len(o.Dir) == 0 {
		errs.Push(ErrEmptyDirectory)
	}

	if len(o.Name) == 0 {
		errs.Push(ErrEmptyName)
	}

	return errs.Err()
}
