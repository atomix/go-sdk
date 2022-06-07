// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newController(runtime *Runtime) *Controller {
	return &Controller{runtime}
}

type Controller struct {
	runtime *Runtime
}

func (c *Controller) CreateCluster(ctx context.Context, cluster *runtimev1.Cluster) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewClusterServiceClient(conn)
	request := &runtimev1.CreateClusterRequest{
		Cluster: cluster,
	}
	_, err = client.CreateCluster(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *Controller) GetCluster(ctx context.Context, clusterID runtimev1.ClusterId) (*runtimev1.Cluster, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewClusterServiceClient(conn)
	request := &runtimev1.GetClusterRequest{
		ClusterID: clusterID,
	}
	response, err := client.GetCluster(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return response.Cluster, nil
}

func (c *Controller) DeleteCluster(ctx context.Context, cluster *runtimev1.Cluster) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewClusterServiceClient(conn)
	request := &runtimev1.DeleteClusterRequest{
		Cluster: cluster,
	}
	_, err = client.DeleteCluster(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *Controller) CreateApplication(ctx context.Context, application *runtimev1.Application) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewApplicationServiceClient(conn)
	request := &runtimev1.CreateApplicationRequest{
		Application: application,
	}
	_, err = client.CreateApplication(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *Controller) GetApplication(ctx context.Context, clusterID runtimev1.ApplicationId) (*runtimev1.Application, error) {
	conn, err := c.connect(ctx)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewApplicationServiceClient(conn)
	request := &runtimev1.GetApplicationRequest{
		ApplicationID: clusterID,
	}
	response, err := client.GetApplication(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return response.Application, nil
}

func (c *Controller) DeleteApplication(ctx context.Context, application *runtimev1.Application) error {
	conn, err := c.connect(ctx)
	if err != nil {
		return errors.FromProto(err)
	}
	defer conn.Close()
	client := runtimev1.NewApplicationServiceClient(conn)
	request := &runtimev1.DeleteApplicationRequest{
		Application: application,
	}
	_, err = client.DeleteApplication(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *Controller) connect(ctx context.Context) (*grpc.ClientConn, error) {
	target := fmt.Sprintf(":%d", c.runtime.ControlService.Port)
	return grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.runtime.Network().Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}
