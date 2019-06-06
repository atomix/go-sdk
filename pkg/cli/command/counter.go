package command

import (
	"github.com/atomix/atomix-go-client/pkg/client/counter"
	"github.com/spf13/cobra"
	"strconv"
)

func newCounterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "counter {create,get,set,increment,decrement,delete}",
		Short: "Manage the state of a distributed counter",
	}
	addClientFlags(cmd)
	cmd.AddCommand(newCounterCreateCommand())
	cmd.AddCommand(newCounterGetCommand())
	cmd.AddCommand(newCounterSetCommand())
	cmd.AddCommand(newCounterIncrementCommand())
	cmd.AddCommand(newCounterDecrementCommand())
	cmd.AddCommand(newCounterDeleteCommand())
	return cmd
}

func newCounterFromName(name string) counter.Counter {
	group := newGroupFromName(name)
	m, err := group.GetCounter(newTimeoutContext(), getPrimitiveName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return m
}

func newCounterCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "create <counter>",
		Args: cobra.ExactArgs(1),
		Run:  runCounterCreateCommand,
	}
}

func runCounterCreateCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	counter.Close()
	ExitWithOutput("Created %s", counter.Name().String())
}

func newCounterDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <counter>",
		Args: cobra.ExactArgs(1),
		Run:  runCounterDeleteCommand,
	}
}

func runCounterDeleteCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	err := counter.Delete()
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput("Deleted %s", counter.Name().String())
	}
}

func newCounterGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "get <counter>",
		Args: cobra.ExactArgs(1),
		Run:  runCounterGetCommand,
	}
}

func runCounterGetCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	value, err := counter.Get(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(value)
	}
}

func newCounterSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "set <counter> <value>",
		Args: cobra.ExactArgs(2),
		Run:  runCounterSetCommand,
	}
}

func runCounterSetCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	value, err := strconv.ParseInt(args[1], 0, 64)
	if err != nil {
		ExitWithError(ExitError, err)
	}
	err = counter.Set(newTimeoutContext(), value)
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(value)
	}
}

func newCounterIncrementCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "increment <counter> [delta]",
		Args: cobra.RangeArgs(1, 2),
		Run:  runCounterIncrementCommand,
	}
}

func runCounterIncrementCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	var delta int64
	var err error
	if len(args) == 1 {
		delta = 1
	} else {
		delta, err = strconv.ParseInt(args[1], 0, 64)
		if err != nil {
			ExitWithError(ExitError, err)
		}
	}

	value, err := counter.Increment(newTimeoutContext(), delta)
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(value)
	}
}

func newCounterDecrementCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "decrement <counter> [delta]",
		Args: cobra.RangeArgs(1, 2),
		Run:  runCounterDecrementCommand,
	}
}

func runCounterDecrementCommand(cmd *cobra.Command, args []string) {
	counter := newCounterFromName(args[0])
	var delta int64
	var err error
	if len(args) == 1 {
		delta = 1
	} else {
		delta, err = strconv.ParseInt(args[1], 0, 64)
		if err != nil {
			ExitWithError(ExitError, err)
		}
	}

	value, err := counter.Decrement(newTimeoutContext(), delta)
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(value)
	}
}
