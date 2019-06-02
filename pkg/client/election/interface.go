package election

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
)

// Interface is the interface for the leader election primitive
type Interface interface {
	primitive.Interface
	GetTerm(ctx context.Context) (*Term, error)
	Enter(ctx context.Context) (*Term, error)
	Leave(ctx context.Context) error
	Anoint(ctx context.Context, id string) (bool, error)
	Promote(ctx context.Context, id string) (bool, error)
	Evict(ctx context.Context, id string) (bool, error)
	Listen(ctx context.Context, c chan<- *ElectionEvent) error
}

type Term struct {
	Term       uint64
	Leader     string
	Candidates []string
}

type ElectionEventType string

const (
	EVENT_CHANGED ElectionEventType = "changed"
)

type ElectionEvent struct {
	Type ElectionEventType
	Term Term
}
