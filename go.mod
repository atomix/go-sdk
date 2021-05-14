module github.com/atomix/atomix-go-client

go 1.13

require (
	github.com/atomix/api v0.3.3
	github.com/atomix/atomix-api/go v0.4.0
	github.com/atomix/atomix-go-framework v0.5.1
	github.com/atomix/atomix-go-local v0.5.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/google/uuid v1.1.2
	github.com/hashicorp/golang-lru v0.5.4
	github.com/stretchr/testify v1.6.1
	google.golang.org/grpc v1.33.2
)

replace github.com/atomix/atomix-api/go => ../atomix-api/go

replace github.com/atomix/atomix-go-framework => ../atomix-go-node

replace github.com/atomix/atomix-go-local => ../atomix-go-local
