package primitive

import "fmt"

func NewName(namespace string, group string, application string, name string) Name {
	return Name{
		Namespace:   namespace,
		Group:       group,
		Application: application,
		Name:        name,
	}
}

type Name struct {
	Namespace   string
	Group       string
	Application string
	Name        string
}

func (n Name) String() string {
	return fmt.Sprintf("%s.%s.%s.%s", n.Namespace, n.Group, n.Application, n.Name)
}

// Primitive is the base interface for primitives
type Primitive interface {
	// Name returns the fully namespaced primitive name
	Name() Name

	// Close closes the primitive
	Close() error

	// Delete deletes the primitive state from the cluster
	Delete() error
}
