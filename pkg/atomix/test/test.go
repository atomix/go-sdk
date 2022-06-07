// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	"github.com/atomix/drivers/memory"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"github.com/atomix/runtime/pkg/atomix/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"time"
)

const (
	clusterName = "test-cluster"
	appName     = "test-app"
	envKey      = "Environment"
	testEnv     = "test"
)

func Context() context.Context {
	return metadata.AppendToOutgoingContext(context.TODO(), envKey, testEnv)
}

func NewRuntime(kind primitive.Kind) *Runtime {
	return &Runtime{
		Runtime: runtime.New(
			runtime.NewLocalNetwork(),
			runtime.WithDrivers(memory.Driver),
			runtime.WithProxyKinds(kind)),
	}
}

type Runtime struct {
	*runtime.Runtime
}

func (r *Runtime) Start() error {
	if err := r.Runtime.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := r.createCluster(ctx); err != nil {
		return err
	}
	if err := r.createApplication(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	target := fmt.Sprintf(":%d", r.ProxyService.Port)
	return r.connect(ctx, target)
}

func (r *Runtime) createCluster(ctx context.Context) error {
	controller := newController(r)
	cluster := &runtimev1.Cluster{
		ClusterMeta: runtimev1.ClusterMeta{
			ID: runtimev1.ClusterId{
				Name: clusterName,
			},
		},
		Spec: runtimev1.ClusterSpec{
			Driver: runtimev1.DriverId{
				Name:    memory.Driver.Name(),
				Version: memory.Driver.Version(),
			},
		},
	}
	return controller.CreateCluster(ctx, cluster)
}

func (r *Runtime) createApplication(ctx context.Context) error {
	controller := newController(r)
	application := &runtimev1.Application{
		ApplicationMeta: runtimev1.ApplicationMeta{
			ID: runtimev1.ApplicationId{
				Name: appName,
			},
		},
		Spec: runtimev1.ApplicationSpec{
			Bindings: []runtimev1.Binding{
				{
					ClusterID: runtimev1.ClusterId{
						Name: clusterName,
					},
					Rules: []runtimev1.BindingRule{
						{
							Headers: map[string]string{
								envKey: testEnv,
							},
						},
					},
				},
			},
		},
	}
	return controller.CreateApplication(ctx, application)
}

func (r *Runtime) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target,
		grpc.WithContextDialer(r.Network().Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}
