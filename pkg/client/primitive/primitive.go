package primitive

// Primitive is the base interface for primitives
type Primitive interface {
	// Close closes the primitive
	Close() error
}
