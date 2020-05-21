module github.com/atomix/go-client

go 1.13

require (
	github.com/atomix/api v0.1.0
	github.com/atomix/go-framework v0.1.1
	github.com/atomix/go-local v0.1.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/google/uuid v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.27.0
)

replace github.com/atomix/api => ../atomix-api
replace github.com/atomix/go-framework => ../atomix-go-node
replace github.com/atomix/go-local => ../atomix-go-local
