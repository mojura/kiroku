package history

const (
	// TypeWriteAction represents a write action
	TypeWriteAction Type = iota
	// TypeDeleteAction represets a delete action
	TypeDeleteAction
	// TypeMeta represents meta information
	TypeMeta
)

// Type represents a block type
type Type uint8
