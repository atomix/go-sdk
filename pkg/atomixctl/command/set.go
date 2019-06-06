package command

import (
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/set"
	"github.com/spf13/cobra"
)

func newSetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set {create,add,contains,remove,size,clear,delete}",
		Short: "Manage the state of a distributed set",
	}
	addClientFlags(cmd)
	cmd.AddCommand(newSetCreateCommand())
	cmd.AddCommand(newSetAddCommand())
	cmd.AddCommand(newSetContainsCommand())
	cmd.AddCommand(newSetRemoveCommand())
	cmd.AddCommand(newSetSizeCommand())
	cmd.AddCommand(newSetClearCommand())
	cmd.AddCommand(newSetDeleteCommand())
	return cmd
}

func newSetFromName(name string) set.Set {
	group := newGroupFromName(name)
	m, err := group.GetSet(newTimeoutContext(), getPrimitiveName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return m
}

func newSetCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "create <set>",
		Args: cobra.ExactArgs(1),
		Run:  runSetCreateCommand,
	}
}

func runSetCreateCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	set.Close()
	ExitWithOutput(fmt.Sprintf("Created %s", set.Name().String()))
}

func newSetDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <set>",
		Args: cobra.ExactArgs(1),
		Run:  runSetDeleteCommand,
	}
}

func runSetDeleteCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	err := set.Delete()
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(fmt.Sprintf("Deleted %s", set.Name().String()))
	}
}

func newSetAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "add <set> <value>",
		Args: cobra.ExactArgs(2),
		Run:  runSetAddCommand,
	}
}

func runSetAddCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	added, err := set.Add(newTimeoutContext(), args[1])
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(added)
	}
}

func newSetContainsCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "contains <set> <value>",
		Args: cobra.ExactArgs(2),
		Run:  runSetContainsCommand,
	}
}

func runSetContainsCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	contains, err := set.Contains(newTimeoutContext(), args[1])
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(contains)
	}
}

func newSetRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "remove <set> <value>",
		Args: cobra.ExactArgs(2),
		Run:  runSetRemoveCommand,
	}
}

func runSetRemoveCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	removed, err := set.Remove(newTimeoutContext(), args[1])
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(removed)
	}
}

func newSetSizeCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "size <set>",
		Args: cobra.ExactArgs(1),
		Run:  runSetSizeCommand,
	}
}

func runSetSizeCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	size, err := set.Size(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(size)
	}
}

func newSetClearCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "clear <set>",
		Args: cobra.ExactArgs(1),
		Run:  runSetClearCommand,
	}
}

func runSetClearCommand(cmd *cobra.Command, args []string) {
	set := newSetFromName(args[0])
	err := set.Clear(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithSuccess()
	}
}
