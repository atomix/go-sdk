package partition

import (
	"google.golang.org/grpc"
)

func NewPartition(id int, address string, opts ...grpc.DialOption) (*Partition, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &Partition{
		Id:   id,
		Conn: conn,
	}, nil
}

// Partition is a client for a specific partition
type Partition struct {
	Id   int
	Conn *grpc.ClientConn
}
