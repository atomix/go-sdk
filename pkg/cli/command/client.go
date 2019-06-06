package command

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	viper.BindPFlag("group", cmd.PersistentFlags().Lookup("group"))
	viper.SetDefault("group", "")
}

func newTimeoutContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx
}

func newClientFromEnv() *client.Client {
	c, err := client.NewClient(globalFlags.Controller, client.WithNamespace(globalFlags.Namespace), client.WithApplication(globalFlags.Application))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
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
	ns := getClientNamespace()
	app := getPrimitiveApplication(name)
	c, err := client.NewClient(globalFlags.Controller, client.WithNamespace(ns), client.WithApplication(app))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
}

func newGroupFromName(name string) *client.PartitionGroup {
	c := newClientFromName(name)
	group, err := c.GetGroup(newTimeoutContext(), getClientGroup())
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

func getClientNamespace() string {
	nameParts := splitName(clientFlags.Group)
	if len(nameParts) == 2 {
		return nameParts[0]
	}
	return globalFlags.Namespace
}

func getClientGroup() string {
	nameParts := splitName(clientFlags.Group)
	return nameParts[len(nameParts)-1]
}

func getPrimitiveApplication(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 2 {
		return nameParts[0]
	}
	return globalFlags.Application
}

func getPrimitiveName(name string) string {
	nameParts := splitName(name)
	return nameParts[len(nameParts)-1]
}
