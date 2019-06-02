package lock

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
)

type Interface interface {
	primitive.Interface
	Lock(ctx context.Context, opts ...LockOption) (uint64, error)
	Unlock(ctx context.Context, opts ...UnlockOption) (bool, error)
	IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error)
}