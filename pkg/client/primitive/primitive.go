package primitive

// Interface is the base interface for primitives
type Interface interface {
	// Close closes the primitive
	Close() error
}
