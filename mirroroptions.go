package kiroku

import (
	"time"

	"github.com/hatchify/errors"
)

const (
	// DefaultEndOfResultsDelay is the default value for EndOfResultsDelay
	DefaultEndOfResultsDelay = time.Second * 10
)

// MirrorOptions represent Mirror options
type MirrorOptions struct {
	Options

	// OnImport will be called whenever an imported value is merged into the primary Chunk
	OnImport Processor
	// EndOfResultsDelay represents the amount of time to wait before pulling "Next" after receiving empty results
	// Note: Default is 10 seconds
	EndOfResultsDelay time.Duration
}

func (m *MirrorOptions) fill() {
	if m.EndOfResultsDelay == 0 {
		m.EndOfResultsDelay = DefaultEndOfResultsDelay
	}
}

func (m *MirrorOptions) Validate() (err error) {
	var errs errors.ErrorList
	m.fill()
	errs.Push(m.Options.Validate())
	return errs.Err()
}
