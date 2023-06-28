package kiroku

import (
	"fmt"
	"time"

	"github.com/hatchify/errors"
)

const (
	// ErrEmptyDirectory is returned when a directory is empty
	ErrEmptyDirectory = errors.Error("invalid directory, cannot be empty")
	// ErrEmptyName is returned when a name is empty
	ErrEmptyName = errors.Error("invalid name, cannot be empty")
)

const (
	// DefaultEndOfResultsDelay is the default value for EndOfResultsDelay
	DefaultEndOfResultsDelay = time.Second * 10
)

// MakeOptions will create new Options
func MakeOptions(dir, name string) (o Options) {
	o.Dir = dir
	o.Name = name
	return
}

// Options represent Kiroku options
type Options struct {
	Dir       string `toml:"dir" json:"dir"`
	Name      string `toml:"name" json:"name"`
	Namespace string `toml:"namespace" json:"namespace"`

	AvoidImportOnInit  bool `toml:"avoid_import_on_init" json:"avoidImportOnInit"`
	AvoidMergeOnInit   bool `toml:"avoid_merge_on_init" json:"avoidMergeOnInit"`
	AvoidMergeOnClose  bool `toml:"avoid_merge_on_close" json:"avoidMergeOnClose"`
	AvoidExportOnClose bool `toml:"avoid_export_on_close" json:"avoidExportOnClose"`

	IsMirror bool `toml:"is_mirror" json:"isMirror"`
	// EndOfResultsDelay represents the amount of time to wait before pulling "Next" after
	// receiving empty results (Default is 10 seconds).
	// Note: This is only used for Mirrors
	EndOfResultsDelay time.Duration `toml:"end_of_results_delay" json:"endOfResultsDelay"`
}

// Validate ensures that the Options have all the required fields set
func (o *Options) Validate() (err error) {
	var errs errors.ErrorList
	if len(o.Dir) == 0 {
		errs.Push(ErrEmptyDirectory)
	}

	if len(o.Name) == 0 {
		errs.Push(ErrEmptyName)
	}

	o.fill()
	return errs.Err()
}

func (o *Options) FullName() string {
	if len(o.Namespace) == 0 {
		return o.Name
	}

	return fmt.Sprintf("%s_%s", o.Namespace, o.Name)
}

func (o *Options) fill() {
	if o.EndOfResultsDelay == 0 {
		o.EndOfResultsDelay = DefaultEndOfResultsDelay
	}
}
