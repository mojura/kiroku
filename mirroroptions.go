package kiroku

import (
	"time"

	"github.com/hatchify/errors"
)

const (
	// DefaultEndOfResultsDelay is the default value for EndOfResultsDelay
	DefaultEndOfResultsDelay = time.Second * 10
)

// MakeMirrorOptions will create new Mirror Options
func MakeMirrorOptions(dir, name string, onImport Processor) (o MirrorOptions) {
	o.Dir = dir
	o.Name = name
	return
}

// MirrorOptions represent Mirror options
type MirrorOptions struct {
	Options

	// EndOfResultsDelay represents the amount of time to wait before pulling "Next" after receiving empty results
	// Note: Default is 10 seconds
	EndOfResultsDelay time.Duration
}

func (m *MirrorOptions) fill() {
	if m.EndOfResultsDelay == 0 {
		m.EndOfResultsDelay = DefaultEndOfResultsDelay
	}
}

// Validate ensures that the MirrorOptions have all the required fields set
func (m *MirrorOptions) Validate() (err error) {
	var errs errors.ErrorList
	m.fill()
	errs.Push(m.Options.Validate())
	return errs.Err()
}
