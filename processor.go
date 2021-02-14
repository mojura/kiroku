package history

// Processor will process chunks
type Processor func(*Reader) error
