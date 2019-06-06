package command

import (
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/spf13/cobra"
)

func newElectionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "election {create,enter,get,leave,delete}",
		Short: "Managed the state of a distributed leader election",
	}
	addClientFlags(cmd)
	cmd.AddCommand(newElectionCreateCommand())
	cmd.AddCommand(newElectionGetCommand())
	cmd.AddCommand(newElectionEnterCommand())
	cmd.AddCommand(newElectionLeaveCommand())
	cmd.AddCommand(newElectionDeleteCommand())
	return cmd
}

func newElectionFromName(name string) election.Election {
	group := newGroupFromName(name)
	m, err := group.GetElection(newTimeoutContext(), getPrimitiveName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return m
}

func newElectionCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "create <election>",
		Args: cobra.ExactArgs(1),
		Run:  runElectionCreateCommand,
	}
}

func runElectionCreateCommand(cmd *cobra.Command, args []string) {
	election := newElectionFromName(args[0])
	election.Close()
	ExitWithOutput(fmt.Sprintf("Created %s", election.Name().String()))
}

func newElectionDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <election>",
		Args: cobra.ExactArgs(1),
		Run:  runElectionDeleteCommand,
	}
}

func runElectionDeleteCommand(cmd *cobra.Command, args []string) {
	election := newElectionFromName(args[0])
	err := election.Delete()
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(fmt.Sprintf("Deleted %s", election.Name().String()))
	}
}

func newElectionGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "get <election>",
		Args: cobra.ExactArgs(1),
		Run:  runElectionGetCommand,
	}
}

func runElectionGetCommand(cmd *cobra.Command, args []string) {
	election := newElectionFromName(args[0])
	term, err := election.GetTerm(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(term)
	}
}

func newElectionEnterCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "enter <election>",
		Args: cobra.ExactArgs(1),
		Run:  runElectionEnterCommand,
	}
}

func runElectionEnterCommand(cmd *cobra.Command, args []string) {
	election := newElectionFromName(args[0])
	term, err := election.Enter(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(term)
	}
}

func newElectionLeaveCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "leave <election>",
		Args: cobra.ExactArgs(1),
		Run:  runElectionLeaveCommand,
	}
}

func runElectionLeaveCommand(cmd *cobra.Command, args []string) {
	election := newElectionFromName(args[0])
	err := election.Leave(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithSuccess()
	}
}
