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
	// DefaultErrorDelay is the default value for ErrorDelay
	DefaultErrorDelay = time.Second * 30
	// DefaultBatchDuration is the default value for BatchDuration
	DefaultBatchDuration = time.Second * 10
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

	AvoidExportOnClose  bool `toml:"avoid_export_on_close" json:"avoidExportOnClose"`
	AvoidProcessOnClose bool `toml:"avoid_merge_on_close" json:"avoidMergeOnClose"`

	// BatchDuration represents the amount of time to keep a transaction open for a
	// Batch operation
	BatchDuration time.Duration `toml:"batch_duration" json:"batchDuration"`

	// EndOfResultsDelay represents the amount of time to wait before pulling "Next" after
	// receiving empty results (Default is 10 seconds).
	EndOfResultsDelay time.Duration `toml:"end_of_results_delay" json:"endOfResultsDelay"`

	// ErrorDelay represents the amount of time to wait before pulling "Next" after
	// receiving an error
	ErrorDelay time.Duration `toml:"error_delay" json:"errorDelay"`

	// RangeStart will determine the moment in time from which syncs will begin
	RangeStart time.Time `toml:"range_start" json:"rangeStart"`
	// RangeEnd will determine the moment in time from which syncs will end
	// Note: This feature is slated to be implemented within the following
	// release. As of now, this will act as a field placeholder
	RangeEnd time.Time `toml:"range_end" json:"rangeEnd"`
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

	if o.ErrorDelay == 0 {
		o.ErrorDelay = DefaultErrorDelay
	}

	if o.BatchDuration == 0 {
		o.BatchDuration = DefaultBatchDuration
	}
}
