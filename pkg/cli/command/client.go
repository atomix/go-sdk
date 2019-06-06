package command

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

const (
	nameSep = "."
)

var (
	clientFlags = &ClientFlags{}
)

type ClientFlags struct {
	Group   string
	Timeout string
}

func addClientFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&clientFlags.Group, "group", "g", "default", "the partition group name")
	cmd.PersistentFlags().StringVarP(&clientFlags.Timeout, "timeout", "t", "15s", "the operation timeout")
}

func newTimeoutContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx
}

func newClientFromGroup(name string) *client.Client {
	ns := getGroupNamespace(name)
	c, err := client.NewClient(globalFlags.Controller, client.WithNamespace(ns), client.WithApplication(globalFlags.Application))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
}

func newClientFromName(name string) *client.Client {
	ns := getPrimitiveNamespace(name)
	app := getPrimitiveApplication(name)
	c, err := client.NewClient(globalFlags.Controller, client.WithNamespace(ns), client.WithApplication(app))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
}

func newGroupFromName(name string) *client.PartitionGroup {
	c := newClientFromName(name)
	group, err := c.GetGroup(newTimeoutContext(), getPrimitiveGroup(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return group
}

func splitName(name string) []string {
	return strings.Split(name, nameSep)
}

func getGroupNamespace(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 2 {
		return nameParts[0]
	}
	return globalFlags.Namespace
}

func getGroupName(name string) string {
	nameParts := splitName(name)
	return nameParts[len(nameParts)-1]
}

func getPrimitiveNamespace(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 4 {
		return nameParts[0]
	}
	return globalFlags.Namespace
}

func getPrimitiveApplication(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 4 {
		return nameParts[2]
	} else if len(nameParts) == 3 {
		return nameParts[1]
	}
	return globalFlags.Application
}

func getPrimitiveGroup(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 4 {
		return nameParts[1]
	} else if len(nameParts) == 3 {
		return nameParts[0]
	} else if len(nameParts) == 2 {
		return nameParts[0]
	}
	return clientFlags.Group
}

func getPrimitiveName(name string) string {
	nameParts := splitName(name)
	return nameParts[len(nameParts)-1]
}
