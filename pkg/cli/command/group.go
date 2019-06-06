package command

import (
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	"github.com/atomix/atomix-go-client/pkg/client/protocol/log"
	"github.com/atomix/atomix-go-client/pkg/client/protocol/raft"
	"github.com/spf13/cobra"
	"os"
	"text/tabwriter"
)

func newGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "group [get,create,delete]",
		Aliases: []string{
			"groups",
		},
		Short: "Manage partition groups and partitions",
		Run:   runGroupsCommand,
	}
	cmd.AddCommand(newGroupGetCommand())
	cmd.AddCommand(newGroupCreateCommand())
	cmd.AddCommand(newGroupDeleteCommand())
	return cmd
}

func runGroupsCommand(cmd *cobra.Command, args []string) {
	client := newClientFromEnv()
	groups, err := client.GetGroups(newTimeoutContext())
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		writer := new(tabwriter.Writer)
		writer.Init(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
		fmt.Fprintln(writer, "Namespace\tName\tPartitions\tPartition Size\tProtocol")
		for _, group := range groups {
			fmt.Fprintln(writer, "%s\t%s\t%d\t%d\t%s", group.Namespace, group.Name, group.Partitions, group.PartitionSize, group.Protocol)
		}
		fmt.Fprintln(writer)
		writer.Flush()
		ExitWithSuccess()
	}
}

func newGroupGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "get <group>",
		Args: cobra.ExactArgs(1),
		Run:  runGroupGetCommand,
	}
}

func runGroupGetCommand(cmd *cobra.Command, args []string) {
	name := args[0]
	client := newClientFromGroup(name)
	group, err := client.GetGroup(newTimeoutContext(), getGroupName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(group)
	}
}

func newGroupCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "create <group>",
		Args: cobra.ExactArgs(1),
		Run:  runGroupCreateCommand,
	}
	cmd.Flags().String("protocol", "raft", "the protocol to run in the partition group")
	cmd.Flags().IntP("partitions", "p", 1, "the number of partitions to create")
	cmd.Flags().IntP("partitionSize", "s", 1, "the size of partitions in the group")
	return cmd
}

func runGroupCreateCommand(cmd *cobra.Command, args []string) {
	name := args[0]
	client := newClientFromGroup(name)

	partitions, _ := cmd.Flags().GetInt("partitions")
	partitionSize, _ := cmd.Flags().GetInt("partitionSize")
	protocolName, _ := cmd.Flags().GetString("protocol")

	var protocolConfig protocol.Protocol
	switch (protocolName) {
	case "raft":
		protocolConfig = &raft.Protocol{}
	case "log":
		protocolConfig = &log.Protocol{}
	}

	group, err := client.CreateGroup(newTimeoutContext(), getGroupName(name), partitions, partitionSize, protocolConfig)
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithOutput(group)
	}
}

func newGroupDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <group>",
		Args: cobra.ExactArgs(1),
		Run:  runGroupDeleteCommand,
	}
}

func runGroupDeleteCommand(cmd *cobra.Command, args []string) {
	name := args[0]
	client := newClientFromGroup(name)
	err := client.DeleteGroup(newTimeoutContext(), getGroupName(name))
	if err != nil {
		ExitWithError(ExitError, err)
	} else {
		ExitWithSuccess()
	}
}
