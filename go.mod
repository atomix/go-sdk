module github.com/atomix/atomix-go-client

go 1.13

require (
	github.com/atomix/atomix-api/go v0.4.9
	github.com/atomix/atomix-go-framework v0.8.1
	github.com/atomix/atomix-go-local v0.7.0
	github.com/atomix/atomix-raft-storage v0.8.3
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.2
	github.com/stretchr/testify v1.6.1
	google.golang.org/grpc v1.33.2
)

replace github.com/atomix/atomix-go-framework => ../atomix-go-node

replace github.com/atomix/atomix-go-local => ../atomix-go-local

replace github.com/atomix/atomix-raft-storage => ../atomix-raft-storage-controller
