package command

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "config <subcommand>",
	}
	cmd.AddCommand(newConfigGetCommand())
	cmd.AddCommand(newConfigSetCommand())
	cmd.AddCommand(newConfigDeleteCommand())
	return cmd
}

func newConfigGetCommand() *cobra.Command {
	return &cobra.Command{
		Use: "get",
		Args: cobra.ExactArgs(1),
		Run: runConfigGetCommand,
	}
}

func runConfigGetCommand(cmd *cobra.Command, args []string) {
	value := viper.Get(args[0])
	ExitWithOutput(value)
}

func newConfigSetCommand() *cobra.Command {
	return &cobra.Command{
		Use: "set",
		Args: cobra.ExactArgs(2),
		Run: runConfigSetCommand,
	}
}

func runConfigSetCommand(cmd *cobra.Command, args []string) {
	viper.Set(args[0], args[1])
	ExitWithOutput(args[1])
}

func newConfigDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use: "delete",
		Run: runConfigDeleteCommand,
	}
}

func runConfigDeleteCommand(cmd *cobra.Command, args []string) {
	viper.Set(args[0], nil)
	value := viper.Get(args[0])
	ExitWithOutput(value)
}
