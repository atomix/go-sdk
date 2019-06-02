package counter

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	pb "github.com/atomix/atomix-go-client/proto/atomix/counter"
	"google.golang.org/grpc"
)

func NewCounter(namespace string, name string, partitions []*grpc.ClientConn, opts ...session.Option) (*Counter, error) {
	i, err := util.GetPartitionIndex(name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := pb.NewCounterServiceClient(partitions[i])
	session := session.NewSession(namespace, name, &SessionHandler{client: client}, opts...)
	if err := session.Start(); err != nil {
		return nil, err
	}

	return &Counter{
		client:  client,
		session: session,
	}, nil
}

type Counter struct {
	Interface
	client  pb.CounterServiceClient
	session *session.Session
}

func (c *Counter) Get(ctx context.Context) (int64, error) {
	request := &pb.GetRequest{
		Header: c.session.GetHeader(),
	}

	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.UpdateHeader(response.Header)
	return response.Value, nil
}

func (c *Counter) Set(ctx context.Context, value int64) (int64, error) {
	request := &pb.SetRequest{
		Header: c.session.NextHeader(),
		Value:  value,
	}

	response, err := c.client.Set(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.UpdateHeader(response.Header)
	return response.PreviousValue + value, nil
}

func (c *Counter) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &pb.IncrementRequest{
		Header: c.session.NextHeader(),
		Delta:  delta,
	}

	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.UpdateHeader(response.Header)
	return response.NextValue, nil
}

func (c *Counter) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &pb.DecrementRequest{
		Header: c.session.NextHeader(),
		Delta:  delta,
	}

	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.UpdateHeader(response.Header)
	return response.NextValue, nil
}

func (c *Counter) Close() error {
	return c.session.Stop()
}
