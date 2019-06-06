package command

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"time"
)

const (
	nameSep = "."
)

func addClientFlags(cmd *cobra.Command) {
	viper.SetDefault("group", "")
	cmd.PersistentFlags().StringP("group", "g", viper.GetString("group"), fmt.Sprintf("the partition group name (default %s)", viper.GetString("group")))
	cmd.PersistentFlags().StringP("timeout", "t", "15s", "the operation timeout")
	viper.BindPFlag("group", cmd.PersistentFlags().Lookup("group"))
}

func newTimeoutContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx
}

func newClientFromEnv() *client.Client {
	c, err := client.NewClient(
		getClientController(),
		client.WithNamespace(getClientNamespace()),
		client.WithApplication(getClientApp()))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
}

func newClientFromGroup(name string) *client.Client {
	c, err := client.NewClient(
		getClientController(),
		client.WithNamespace(getGroupNamespace(name)),
		client.WithApplication(getClientApp()))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return c
}

func newClientFromName(name string) *client.Client {
	c, err := client.NewClient(getClientController(), client.WithNamespace(getClientNamespace()), client.WithApplication(getPrimitiveApp(name)))
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
	return getClientNamespace()
}

func getGroupName(name string) string {
	nameParts := splitName(name)
	return nameParts[len(nameParts)-1]
}

func getClientController() string {
	return viper.GetString("controller")
}

func getClientNamespace() string {
	return viper.GetString("namespace")
}

func getClientGroup() string {
	return viper.GetString("group")
}

func getClientApp() string {
	return viper.GetString("app")
}

func getPrimitiveApp(name string) string {
	nameParts := splitName(name)
	if len(nameParts) == 2 {
		return nameParts[0]
	}
	return getClientApp()
}

func getPrimitiveName(name string) string {
	nameParts := splitName(name)
	return nameParts[len(nameParts)-1]
}
