package command

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "Read and update CLI configuration options",
	}
	cmd.AddCommand(newConfigGetCommand())
	cmd.AddCommand(newConfigSetCommand())
	cmd.AddCommand(newConfigDeleteCommand())
	return cmd
}

func newConfigGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "get <key>",
		Args: cobra.ExactArgs(1),
		Run:  runConfigGetCommand,
	}
}

func runConfigGetCommand(cmd *cobra.Command, args []string) {
	value := viper.Get(args[0])
	ExitWithOutput(value)
}

func newConfigSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "set <key> <value>",
		Args: cobra.ExactArgs(2),
		Run:  runConfigSetCommand,
	}
}

func runConfigSetCommand(cmd *cobra.Command, args []string) {
	viper.Set(args[0], args[1])
	if err := viper.WriteConfig(); err != nil {
		ExitWithError(ExitError, err)
	} else {
		value := viper.Get(args[0])
		ExitWithOutput(value)
	}
}

func newConfigDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <key>",
		Args: cobra.ExactArgs(1),
		Run:  runConfigDeleteCommand,
	}
}

func runConfigDeleteCommand(cmd *cobra.Command, args []string) {
	viper.Set(args[0], nil)
	if err := viper.WriteConfig(); err != nil {
		ExitWithError(ExitError, err)
	} else {
		value := viper.Get(args[0])
		ExitWithOutput(value)
	}
}

func newInitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize the Atomix CLI configuration",
		Run: func(cmd *cobra.Command, args []string) {
			if err := viper.ReadInConfig(); err == nil {
				ExitWithSuccess()
			}

			home, err := homedir.Dir()
			if err != nil {
				ExitWithError(ExitError, err)
			}

			err = os.MkdirAll(home+"/.atomix", 0777)
			if err != nil {
				ExitWithError(ExitError, err)
			}

			f, err := os.Create(home + "/.atomix/config.yaml")
			if err != nil {
				ExitWithError(ExitError, err)
			} else {
				f.Close()
			}

			err = viper.WriteConfig()
			if err != nil {
				ExitWithError(ExitError, err)
			} else {
				ExitWithSuccess()
			}
		},
	}
}

func initConfig() {
	if globalFlags.Config != "" {
		viper.SetConfigFile(globalFlags.Config)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			ExitWithError(ExitError, err)
		}

		viper.SetConfigName("config")
		viper.AddConfigPath(home + "/.atomix")
		viper.AddConfigPath("/etc/atomix")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration found")
	}
}
