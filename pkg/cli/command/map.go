package command

import (
	"github.com/atomix/atomix-go-client/pkg/client/_map"
	"github.com/spf13/cobra"
)

func newMapCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "map {create,put,get,remove,size,clear,delete}",
	}
	addClientFlags(cmd)
	cmd.AddCommand(newMapCreateCommand())
	cmd.AddCommand(newMapGetCommand())
	cmd.AddCommand(newMapPutCommand())
	cmd.AddCommand(newMapRemoveCommand())
	cmd.AddCommand(newMapSizeCommand())
	cmd.AddCommand(newMapClearCommand())
	cmd.AddCommand(newMapDeleteCommand())
	return cmd
}

func newMapFromName(name string) _map.Map {
	group := newGroupFromName(name)
	m, err := group.GetMap(newTimeoutContext(), getPrimitiveName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return m
}

func newMapCreateCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "create <map>",
		Args: cobra.ExactArgs(1),
		Run:  runMapCreateCommand,
	}
}

func runMapCreateCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	_map.Close()
	ExitWithOutput("Created %s", _map.Name().String())
}

func newMapDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <map>",
		Args: cobra.ExactArgs(1),
		Run:  runMapDeleteCommand,
	}
}

func runMapDeleteCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	err := _map.Delete()
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput("Deleted %s", _map.Name().String())
	}
}

func newMapGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "get <map> <key>",
		Args: cobra.ExactArgs(2),
		Run:  runMapGetCommand,
	}
}

func runMapGetCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	ExitWithOutput(_map.Get(newTimeoutContext(), args[1]))
}

func newMapPutCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "put <map> <key> <value>",
		Args: cobra.ExactArgs(3),
		Run:  runMapPutCommand,
	}
}

func runMapPutCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	ExitWithOutput(_map.Put(newTimeoutContext(), args[1], []byte(args[2])))
}

func newMapRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "remove <map> <key>",
		Args: cobra.ExactArgs(2),
		Run:  runMapRemoveCommand,
	}
}

func runMapRemoveCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	ExitWithOutput(_map.Remove(newTimeoutContext(), args[1]))
}

func newMapSizeCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "size <map>",
		Args: cobra.ExactArgs(1),
		Run:  runMapSizeCommand,
	}
}

func runMapSizeCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	ExitWithOutput(_map.Size(newTimeoutContext()))
}

func newMapClearCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "clear <map>",
		Args: cobra.ExactArgs(1),
		Run:  runMapClearCommand,
	}
}

func runMapClearCommand(cmd *cobra.Command, args []string) {
	_map := newMapFromName(args[0])
	ExitWithOutput(_map.Clear(newTimeoutContext()))
}
