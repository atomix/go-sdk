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
	cobra.OnInitialize(initConfigSettings)
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

	cmd.AddCommand(newInitCommand())
	cmd.AddCommand(configCommand(newConfigCommand()))
	cmd.AddCommand(configCommand(newGroupCommand()))
	cmd.AddCommand(configCommand(newCounterCommand()))
	cmd.AddCommand(configCommand(newElectionCommand()))
	cmd.AddCommand(configCommand(newLockCommand()))
	cmd.AddCommand(configCommand(newMapCommand()))
	cmd.AddCommand(configCommand(newSetCommand()))
	return cmd
}

func configCommand(cmd *cobra.Command) *cobra.Command {
	cmd.PreRun = func(c *cobra.Command, args []string) {
		readConfig()
	}
	return cmd
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

func initConfigSettings() {
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
}

func readConfig() {
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration found")
	}
}
