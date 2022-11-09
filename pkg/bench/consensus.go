// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package bench

import (
	"context"
	"fmt"
	"github.com/atomix/consensus-storage/driver"
	"github.com/atomix/consensus-storage/node/pkg/consensus"
	"github.com/atomix/go-sdk/pkg/primitive"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	electionv1 "github.com/atomix/runtime/primitives/pkg/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/pkg/indexedmap/v1"
	lockv1 "github.com/atomix/runtime/primitives/pkg/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	setv1 "github.com/atomix/runtime/primitives/pkg/set/v1"
	valuev1 "github.com/atomix/runtime/primitives/pkg/value/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	counterv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/counter/v1"
	electionv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/election/v1"
	indexedmapv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/indexedmap/v1"
	listv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/list/v1"
	lockv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/lock/v1"
	mapv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/map/v1"
	setv1proxy "github.com/atomix/runtime/proxy/pkg/proxy/set/v1"
	valuev1proxy "github.com/atomix/runtime/proxy/pkg/proxy/value/v1"
	"github.com/atomix/runtime/sdk/pkg/async"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/lni/dragonboat/v3/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

var log = logging.GetLogger()

const dataDir = "test-data"

var Types = []proxy.Type{
	counterv1proxy.Type,
	electionv1proxy.Type,
	indexedmapv1proxy.Type,
	listv1proxy.Type,
	lockv1proxy.Type,
	mapv1proxy.Type,
	setv1proxy.Type,
	valuev1proxy.Type,
}

func NewConsensusBenchmark(numReplicas, numPartitions int) primitive.Client {
	_ = os.RemoveAll(dataDir)

	logger.GetLogger("raft").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	network := network.NewLocalNetwork()
	return &consensusBenchmark{
		network:       network,
		types:         Types,
		numReplicas:   numReplicas,
		numPartitions: numPartitions,
	}
}

type consensusBenchmark struct {
	network       network.Network
	numReplicas   int
	numPartitions int
	nodes         map[consensus.MemberID]*node.Node
	protocols     map[consensus.MemberID]*consensus.Protocol
	types         []proxy.Type
	proxies       []*proxy.Proxy
	mu            sync.Mutex
}

func (c *consensusBenchmark) startNode(memberID consensus.MemberID) error {
	registry := statemachine.NewPrimitiveTypeRegistry()
	counterv1.RegisterStateMachine(registry)
	electionv1.RegisterStateMachine(registry)
	indexedmapv1.RegisterStateMachine(registry)
	lockv1.RegisterStateMachine(registry)
	mapv1.RegisterStateMachine(registry)
	setv1.RegisterStateMachine(registry)
	valuev1.RegisterStateMachine(registry)

	nodeDir := filepath.Join(dataDir, strconv.Itoa(int(memberID)))
	protocol := consensus.NewProtocol(
		consensus.RaftConfig{
			DataDir: &nodeDir,
		},
		registry,
		consensus.WithHost("localhost"),
		consensus.WithPort(6000+int(memberID)))
	node := node.NewNode(
		c.network,
		protocol,
		node.WithHost("localhost"),
		node.WithPort(nextPort()))

	counterv1.RegisterServer(node)
	electionv1.RegisterServer(node)
	indexedmapv1.RegisterServer(node)
	lockv1.RegisterServer(node)
	mapv1.RegisterServer(node)
	setv1.RegisterServer(node)
	valuev1.RegisterServer(node)

	if err := node.Start(); err != nil {
		return err
	}
	println(fmt.Sprintf("PORT: %d", node.Port))

	members := make([]consensus.MemberConfig, c.numReplicas)
	for i := 0; i < c.numReplicas; i++ {
		members[i] = consensus.MemberConfig{
			MemberID: consensus.MemberID(i + 1),
			Host:     "localhost",
			Port:     int32(6000 + i + 1),
		}
	}

	return async.IterAsync(c.numPartitions, func(i int) error {
		err := protocol.Bootstrap(consensus.GroupConfig{
			GroupID:  consensus.GroupID(i + 1),
			MemberID: memberID,
			Role:     consensus.MemberRole_MEMBER,
			Members:  members,
		})
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.nodes[memberID] = node
		c.protocols[memberID] = protocol
		c.mu.Unlock()
		return nil
	})
}

func (c *consensusBenchmark) start() error {
	if c.nodes != nil {
		return nil
	}

	c.nodes = make(map[consensus.MemberID]*node.Node)
	c.protocols = make(map[consensus.MemberID]*consensus.Protocol)

	return async.IterAsync(c.numReplicas, func(i int) error {
		return c.startNode(consensus.MemberID(i + 1))
	})
}

func (c *consensusBenchmark) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := c.start(); err != nil {
		return nil, err
	}

	driver := driver.New(c.network)
	proxy := proxy.New(c.network,
		proxy.WithDrivers(driver),
		proxy.WithTypes(c.types...),
		proxy.WithProxyPort(nextPort()),
		proxy.WithRuntimePort(nextPort()),
		proxy.WithConfig(proxy.Config{
			Router: proxy.RouterConfig{
				Routes: []proxy.RouteConfig{
					{
						Store: proxy.StoreID{
							Name: "test",
						},
					},
				},
			},
		}))

	if err := proxy.Start(); err != nil {
		return nil, err
	}
	c.proxies = append(c.proxies, proxy)

	conn, err := c.connect(ctx, fmt.Sprintf(":%d", proxy.ProxyService.Port))
	if err != nil {
		return nil, err
	}

	client := proxyv1.NewProxyClient(conn)

	c.mu.Lock()
	members := make(map[consensus.MemberID]string)
	for memberID, node := range c.nodes {
		members[memberID] = fmt.Sprintf("%s:%d", node.Host, node.Port)
	}

	followers := make([]string, 0, len(members))
	for _, address := range members {
		followers = append(followers, address)
	}

	var config protocol.ProtocolConfig
	for i := 1; i <= c.numPartitions; i++ {
		config.Partitions = append(config.Partitions, protocol.PartitionConfig{
			PartitionID: protocol.PartitionID(i),
			Followers:   followers,
		})
	}

	for _, p := range c.protocols {
		ch := make(chan consensus.Event)
		go p.Watch(context.Background(), ch)
		go func(config protocol.ProtocolConfig) {
			for event := range ch {
				switch e := event.Event.(type) {
				case *consensus.Event_LeaderUpdated:
					partitionID := int(e.LeaderUpdated.GroupID)
					partitionConfig := protocol.PartitionConfig{
						PartitionID: protocol.PartitionID(partitionID),
					}
					if e.LeaderUpdated.Leader > 0 {
						partitionConfig.Leader = members[e.LeaderUpdated.Leader]
					}
					for memberID, address := range members {
						if e.LeaderUpdated.Leader != memberID {
							partitionConfig.Followers = append(partitionConfig.Followers, address)
						}
					}
					config.Partitions[partitionID-1] = partitionConfig

					log.Infof("Updating ProtocolConfig: %s", config)

					marshaller := &jsonpb.Marshaler{}
					data, err := marshaller.MarshalToString(&config)
					if err != nil {
						log.Error(err)
						continue
					}

					request := &proxyv1.ConfigureRequest{
						StoreID: proxyv1.StoreId{
							Name: "test",
						},
						Config: []byte(data),
					}
					_, err = client.Configure(ctx, request)
					if err != nil {
						log.Error(err)
						continue
					}
				}
			}
		}(config)
	}
	c.mu.Unlock()

	marshaller := &jsonpb.Marshaler{}
	data, err := marshaller.MarshalToString(&config)
	if err != nil {
		return nil, err
	}

	request := &proxyv1.ConnectRequest{
		StoreID: proxyv1.StoreId{
			Name: "test",
		},
		DriverID: proxyv1.DriverId{
			Name:    driver.ID().Name,
			Version: driver.ID().Version,
		},
		Config: []byte(data),
	}
	_, err = client.Connect(ctx, request)
	if err != nil {
		return nil, err
	}
	return c.connect(ctx, fmt.Sprintf(":%d", proxy.RuntimeService.Port))
}

func (c *consensusBenchmark) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (c *consensusBenchmark) Close() {
	for _, proxy := range c.proxies {
		_ = proxy.Stop()
	}
	for _, node := range c.nodes {
		_ = node.Stop()
	}
	_ = os.RemoveAll(dataDir)
}

var port = &atomic.Int32{}

func init() {
	port.Store(5000)
}

func nextPort() int {
	return int(port.Add(1))
}
