module github.com/atomix/go-sdk

go 1.19

require (
	github.com/atomix/consensus-storage/driver v0.13.1
	github.com/atomix/consensus-storage/node v0.13.1
	github.com/atomix/runtime/api v0.7.0
	github.com/atomix/runtime/primitives v0.7.8
	github.com/atomix/runtime/proxy v0.10.0
	github.com/atomix/runtime/sdk v0.7.6
	github.com/atomix/shared-memory-storage/driver v0.1.3
	github.com/atomix/shared-memory-storage/node v0.1.2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/lni/dragonboat/v3 v3.3.5
	github.com/stretchr/testify v1.7.1
	google.golang.org/grpc v1.46.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

require (
	github.com/VictoriaMetrics/metrics v1.6.2 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/bits-and-blooms/bloom/v3 v3.2.0 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cockroachdb/errors v1.7.5 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/pebble v0.0.0-20210331181633-27fc006b8bfb // indirect
	github.com/cockroachdb/redact v1.0.6 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/snappy v0.0.3-0.20201103224600-674baa8c7fc3 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/memberlist v0.2.2 // indirect
	github.com/juju/ratelimit v1.0.2-0.20191002062651-f60b32039441 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/lni/goutils v1.3.0 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/valyala/fastrand v1.0.0 // indirect
	github.com/valyala/histogram v1.0.1 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/net v0.0.0-20220412020605-290c469a71a5 // indirect
	golang.org/x/sys v0.0.0-20220503163025-988cb79eb6c6 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220407144326-9054f6ed7bac // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)

replace (
	github.com/atomix/consensus-storage/driver => ../../storage/consensus/driver
	github.com/atomix/consensus-storage/node => ../../storage/consensus/node
	github.com/atomix/runtime/primitives => ../../runtime/primitives
	github.com/atomix/runtime/proxy => ../../runtime/proxy
	github.com/atomix/runtime/sdk => ../../runtime/sdk
	github.com/atomix/shared-memory-storage/driver => ../../storage/shared-memory/driver
	github.com/atomix/shared-memory-storage/node => ../../storage/shared-memory/node
)
