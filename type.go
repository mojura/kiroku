package kiroku

import "fmt"

const (
	// TypeWriteAction represents a write action block
	TypeWriteAction Type = iota
	// TypeDeleteAction represets a delete action block
	TypeDeleteAction
	// TypeComment represents a comment block
	TypeComment
)

// Type represents a block type
type Type uint8

// Validate will ensure a type is valid
func (t Type) Validate() (err error) {
	switch t {
	case TypeWriteAction:
	case TypeDeleteAction:
	case TypeComment:

	default:
		// Currently set as an unsupported type, return error
		return fmt.Errorf("invalid type, <%d> is not supported", t)
	}

	return
}

// Validate will ensure a type is valid
func (t Type) String() string {
	switch t {
	case TypeWriteAction:
		return "write"
	case TypeDeleteAction:
		return "delete"
	case TypeComment:
		return "comment"

	default:
		// Currently st as an unsupported type, return error
		return "invalid"
	}
}
