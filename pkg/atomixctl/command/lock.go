package command

import (
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/spf13/cobra"
)

func newLockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lock {create,lock,get,unlock,delete}",
		Short: "Manage the state of a distributed lock",
	}
	addClientFlags(cmd)
	cmd.AddCommand(newLockCreateCommand())
	cmd.AddCommand(newLockLockCommand())
	cmd.AddCommand(newLockGetCommand())
	cmd.AddCommand(newLockUnlockCommand())
	cmd.AddCommand(newLockDeleteCommand())
	return cmd
}

func newLockFromName(name string) lock.Lock {
	group := newGroupFromName(name)
	m, err := group.GetLock(newTimeoutContext(), getPrimitiveName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return m
}

func newLockCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "create <lock>",
		Args: cobra.ExactArgs(1),
		Run:  runLockCreateCommand,
	}
}

func runLockCreateCommand(cmd *cobra.Command, args []string) {
	lock := newLockFromName(args[0])
	lock.Close()
	ExitWithOutput(fmt.Sprintf("Created %s", lock.Name().String()))
}

func newLockDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <lock>",
		Args: cobra.ExactArgs(1),
		Run:  runLockDeleteCommand,
	}
}

func runLockDeleteCommand(cmd *cobra.Command, args []string) {
	lock := newLockFromName(args[0])
	err := lock.Delete()
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(fmt.Sprintf("Deleted %s", lock.Name().String()))
	}
}

func newLockLockCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "lock <lock>",
		Args: cobra.ExactArgs(1),
		Run:  runLockLockCommand,
	}
}

func runLockLockCommand(cmd *cobra.Command, args []string) {
	lock := newLockFromName(args[0])
	version, err := lock.Lock(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(version)
	}
}

func newLockGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "get <lock>",
		Args: cobra.ExactArgs(1),
		Run:  runLockGetCommand,
	}
	cmd.Flags().Uint64P("version", "v", 0, "the lock version")
	return cmd
}

func runLockGetCommand(cmd *cobra.Command, args []string) {
	l := newLockFromName(args[0])
	version, _ := cmd.Flags().GetUint64("version")
	if version == 0 {
		locked, err := l.IsLocked(newTimeoutContext())
		if err != nil {
			ExitWithError(ExitError, err)
		} else {
			ExitWithOutput(locked)
		}
	} else {
		locked, err := l.IsLocked(newTimeoutContext(), lock.WithIsVersion(version))
		if err != nil {
			ExitWithError(ExitError, err)
		} else {
			ExitWithOutput(locked)
		}
	}
}

func newLockUnlockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "unlock <lock>",
		Args: cobra.ExactArgs(1),
		Run:  runLockUnlockCommand,
	}
	cmd.Flags().Uint64P("version", "v", 0, "the lock version")
	return cmd
}

func runLockUnlockCommand(cmd *cobra.Command, args []string) {
	l := newLockFromName(args[0])
	version, _ := cmd.Flags().GetUint64("version")
	if version == 0 {
		unlocked, err := l.Unlock(newTimeoutContext())
		if err != nil {
			ExitWithError(ExitError, err)
		} else {
			ExitWithOutput(unlocked)
		}
	} else {
		unlocked, err := l.Unlock(newTimeoutContext(), lock.WithVersion(version))
		if err != nil {
			ExitWithError(ExitError, err)
		} else {
			ExitWithOutput(unlocked)
		}
	}
}
