package command

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func GetRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "atomixctl",
		Short: "Atomix command line client",
	}

	viper.SetDefault("controller", ":5679")
	viper.SetDefault("namespace", "default")
	viper.SetDefault("app", "default")

	cmd.PersistentFlags().StringP("controller", "c", viper.GetString("controller"), "the controller address")
	cmd.PersistentFlags().StringP("namespace", "n", viper.GetString("namespace"), "the partition group namespace")
	cmd.PersistentFlags().StringP("app", "a", viper.GetString("app"), "the application name")
	cmd.PersistentFlags().String("config", "", "config file (default: $HOME/.atomix/config.yaml)")

	viper.BindPFlag("controller", cmd.PersistentFlags().Lookup("controller"))
	viper.BindPFlag("namespace", cmd.PersistentFlags().Lookup("namespace"))
	viper.BindPFlag("app", cmd.PersistentFlags().Lookup("app"))

	cmd.AddCommand(newInitCommand())
	cmd.AddCommand(newCompletionCommand())
	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newGroupCommand())
	cmd.AddCommand(newCounterCommand())
	cmd.AddCommand(newElectionCommand())
	cmd.AddCommand(newLockCommand())
	cmd.AddCommand(newMapCommand())
	cmd.AddCommand(newSetCommand())
	return cmd
}
