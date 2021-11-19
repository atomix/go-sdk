module github.com/atomix/atomix-go-client

go 1.13

require (
	github.com/atomix/atomix-api/go v0.7.0
	github.com/atomix/atomix-sdk-go v0.0.0-20211103073703-8ea4bd1484be
	github.com/google/uuid v1.1.2
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.38.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/atomix/atomix-sdk-go => ../atomix-go-sdk
