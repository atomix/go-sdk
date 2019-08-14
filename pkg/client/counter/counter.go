package counter

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	pb "github.com/atomix/atomix-go-client/proto/atomix/counter"
	"google.golang.org/grpc"
)

type CounterClient interface {
	GetCounter(ctx context.Context, name string, opts ...session.SessionOption) (Counter, error)
}

// Counter is the interface for the counter primitive
type Counter interface {
	primitive.Primitive
	Get(ctx context.Context) (int64, error)
	Set(ctx context.Context, value int64) error
	Increment(ctx context.Context, delta int64) (int64, error)
	Decrement(ctx context.Context, delta int64) (int64, error)
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Counter, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := pb.NewCounterServiceClient(partitions[i])
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	return &counter{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type counter struct {
	name    primitive.Name
	client  pb.CounterServiceClient
	session *session.Session
}

func (c *counter) Name() primitive.Name {
	return c.name
}

func (c *counter) Get(ctx context.Context) (int64, error) {
	request := &pb.GetRequest{
		Header: c.session.GetRequest(),
	}

	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.Value, nil
}

func (c *counter) Set(ctx context.Context, value int64) error {
	request := &pb.SetRequest{
		Header: c.session.NextRequest(),
		Value:  value,
	}

	response, err := c.client.Set(ctx, request)
	if err != nil {
		return err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (c *counter) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &pb.IncrementRequest{
		Header: c.session.NextRequest(),
		Delta:  delta,
	}

	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.NextValue, nil
}

func (c *counter) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &pb.DecrementRequest{
		Header: c.session.NextRequest(),
		Delta:  delta,
	}

	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.NextValue, nil
}

func (c *counter) Close() error {
	return c.session.Close()
}

func (c *counter) Delete() error {
	return c.session.Delete()
}
