package counter

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
)

// Interface is the interface for the counter primitive
type Interface interface {
	primitive.Interface
	Get(ctx context.Context) (int64, error)
	Set(ctx context.Context, value int64) error
	Increment(ctx context.Context, delta int64) (int64, error)
	Decrement(ctx context.Context, delta int64) (int64, error)
}
