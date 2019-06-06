package command

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func GetRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "atomix",
		Short: "Atomix command line client",
	}

	viper.SetDefault("controller", ":5679")
	viper.SetDefault("namespace", "default")
	viper.SetDefault("app", "default")

	cmd.PersistentFlags().StringP("controller", "c", viper.GetString("controller"), fmt.Sprintf("the controller address (default: %s)", viper.GetString("controller")))
	cmd.PersistentFlags().StringP("namespace", "n", viper.GetString("namespace"), fmt.Sprintf("the partition group namespace (default: %s)", viper.GetString("namespace")))
	cmd.PersistentFlags().StringP("app", "a", viper.GetString("app"), fmt.Sprintf("the application name (default: %s)", viper.GetString("app")))
	cmd.PersistentFlags().String("config", "", "config file (default: $HOME/.atomix/config.yaml)")

	viper.BindPFlag("controller", cmd.PersistentFlags().Lookup("controller"))
	viper.BindPFlag("namespace", cmd.PersistentFlags().Lookup("namespace"))
	viper.BindPFlag("app", cmd.PersistentFlags().Lookup("app"))

	cmd.AddCommand(newInitCommand())
	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newGroupCommand())
	cmd.AddCommand(newCounterCommand())
	cmd.AddCommand(newElectionCommand())
	cmd.AddCommand(newLockCommand())
	cmd.AddCommand(newMapCommand())
	cmd.AddCommand(newSetCommand())
	return cmd
}
