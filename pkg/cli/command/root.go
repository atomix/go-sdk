package command

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var (
	globalFlags = &GlobalFlags{}
)

type GlobalFlags struct {
	Controller  string
	Namespace   string
	Application string
	Config      string
}

func init() {
	cobra.OnInitialize(initConfig)
}

func GetRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "atomix",
		Short: "Atomix command line client",
	}

	cmd.PersistentFlags().StringVarP(&globalFlags.Controller, "controller", "c", ":5679", "The controller address")
	cmd.PersistentFlags().StringVarP(&globalFlags.Namespace, "namespace", "n", "default", "The partition group namespace")
	cmd.PersistentFlags().StringVarP(&globalFlags.Application, "application", "a", "default", "The application name")
	cmd.PersistentFlags().StringVar(&globalFlags.Config, "config", "", "config file (default is $HOME/.atomix/config.yaml)")

	viper.BindPFlag("controller", cmd.PersistentFlags().Lookup("controller"))
	viper.BindPFlag("namespace", cmd.PersistentFlags().Lookup("namespace"))
	viper.BindPFlag("application", cmd.PersistentFlags().Lookup("application"))

	viper.SetDefault("controller", ":5679")
	viper.SetDefault("namespace", "default")
	viper.SetDefault("application", "default")
	viper.SetDefault("group", "")

	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newGroupCommand())
	cmd.AddCommand(newMapCommand())
	cmd.AddCommand(newSetCommand())
	return cmd
}

func initConfig() {
	if globalFlags.Config != "" {
		viper.SetConfigFile(globalFlags.Config)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.SetConfigName("config")
		viper.AddConfigPath("/etc/atomix")
		viper.AddConfigPath(home + "/.atomix")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Cannot read config:", err)
		os.Exit(1)
	}
}
