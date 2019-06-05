package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	logp "github.com/atomix/atomix-go-client/pkg/client/protocol/log"
	"github.com/atomix/atomix-go-client/pkg/client/protocol/raft"
	"log"
	"os"
)

func main() {
	commands := getCommands()
	for _, cmd := range commands {
		if err := cmd.init(); err != nil {
			log.Fatal("Failed to initialize command parser", err)
		}
	}

	if len(os.Args) < 2 {
		log.Fatal("No command specified")
	}

	name := os.Args[1]
	for _, cmd := range commands {
		if cmd.name() == name {
			if err := cmd.parse(os.Args[2:]); err != nil {
				log.Fatal("Failed to parse command", err)
			}
			if err := cmd.execute(); err != nil {
				log.Fatal("Failed to execute command", err)
			}
		}
	}
}

func getCommands() []command {
	return []command{
		newGroupCommand(),
		newMapCommand(),
	}
}

type command interface {
	name() string
	init() error
	parse(args []string) error
	execute() error
}

func newGroupCommand() command {
	return &groupCommand{
		actions: getGroupActions(),
	}
}

type groupCommand struct {
	command
	actions []command
	action  command
}

func (c *groupCommand) name() string {
	return "group"
}

func (c *groupCommand) init() error {
	for _, action := range c.actions {
		if err := action.init(); err != nil {
			return err
		}
	}
	return nil
}

func (c *groupCommand) parse(args []string) error {
	name := args[0]
	for _, action := range c.actions {
		if "-"+action.name() == name {
			c.action = action
			return action.parse(args[1:])
		}
	}
	return nil
}

func (c *groupCommand) execute() error {
	return c.action.execute()
}

func getGroupActions() []command {
	return []command{
		getGroupGetCommand(),
		getGroupCreateCommand(),
	}
}

func getGroupGetCommand() command {
	return &groupGetCommand{
		flag: flag.NewFlagSet("get", flag.ExitOnError),
	}
}

type groupGetCommand struct {
	command
	flag       *flag.FlagSet
	controller *string
	namespace  *string
	group      *string
}

func (c *groupGetCommand) name() string {
	return "get"
}

func (c *groupGetCommand) init() error {
	c.controller = c.flag.String("controller", "localhost:5679", "the controller address")
	c.namespace = c.flag.String("namespace", "", "the partition group namespace")
	c.group = c.flag.String("group", "", "the partition group name")
	return nil
}

func (c *groupGetCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *groupGetCommand) execute() error {
	cl, err := client.NewClient(*c.controller, client.WithNamespace(*c.namespace))
	if err != nil {
		return err
	}

	group, err := cl.GetGroup(context.Background(), *c.group)
	if err != nil {
		return err
	}

	if group != nil {
		println(fmt.Sprintf("%+v", *group))
	}
	return nil
}

func getGroupCreateCommand() command {
	return &groupCreateCommand{
		flag: flag.NewFlagSet("create", flag.ExitOnError),
	}
}

type groupCreateCommand struct {
	command
	flag          *flag.FlagSet
	controller    *string
	namespace     *string
	group         *string
	partitions    *int
	partitionSize *int
	protocol      *string
}

func (c *groupCreateCommand) name() string {
	return "put"
}

func (c *groupCreateCommand) init() error {
	c.controller = c.flag.String("controller", "localhost:5679", "the controller address")
	c.namespace = c.flag.String("namespace", "", "the partition group namespace")
	c.group = c.flag.String("group", "", "the partition group name")
	c.partitions = c.flag.Int("partitions", 1, "the number of partitions to create")
	c.partitionSize = c.flag.Int("partitionSize", 1, "the size of each partition")
	c.protocol = c.flag.String("protocol", "raft", "the protocol to run")
	return nil
}

func (c *groupCreateCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *groupCreateCommand) execute() error {
	cl, err := client.NewClient(*c.controller, client.WithNamespace(*c.namespace))
	if err != nil {
		return err
	}

	var p protocol.Protocol
	switch *c.protocol {
	case "raft":
		p = &raft.Protocol{}
	case "log":
		p = &logp.Protocol{}
	}

	group, err := cl.CreateGroup(context.Background(), *c.group, *c.partitions, *c.partitionSize, p)
	if err != nil {
		return err
	}

	if group != nil {
		println(fmt.Sprintf("%+v", *group))
	}
	return nil
}

func newMapCommand() command {
	return &mapCommand{
		actions: getMapActions(),
	}
}

type mapCommand struct {
	command
	actions []command
	action  command
}

func (c *mapCommand) name() string {
	return "map"
}

func (c *mapCommand) init() error {
	for _, action := range c.actions {
		if err := action.init(); err != nil {
			return err
		}
	}
	return nil
}

func (c *mapCommand) parse(args []string) error {
	name := args[0]
	for _, action := range c.actions {
		if "-"+action.name() == name {
			c.action = action
			return action.parse(args[1:])
		}
	}
	return nil
}

func (c *mapCommand) execute() error {
	return c.action.execute()
}

func getMapActions() []command {
	return []command{
		getMapGetCommand(),
		getMapPutCommand(),
	}
}

func getMapGetCommand() command {
	return &mapGetCommand{
		flag: flag.NewFlagSet("get", flag.ExitOnError),
	}
}

type mapGetCommand struct {
	command
	flag       *flag.FlagSet
	controller *string
	namespace  *string
	group      *string
	app        *string
	_name      *string
	key        *string
}

func (c *mapGetCommand) name() string {
	return "get"
}

func (c *mapGetCommand) init() error {
	c.controller = c.flag.String("controller", "", "the controller for the group")
	c.namespace = c.flag.String("namespace", "default", "the partition group namespace")
	c.group = c.flag.String("group", "", "the partition group name")
	c._name = c.flag.String("name", "", "the map name")
	c.key = c.flag.String("key", "", "the key to get")
	return nil
}

func (c *mapGetCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *mapGetCommand) execute() error {
	var group *client.PartitionGroup
	var err error
	if *c.controller != "" {
		cl, err := client.NewClient(*c.controller, client.WithApplication(*c.app), client.WithNamespace(*c.namespace))
		if err != nil {
			return err
		}
		group, err = cl.GetGroup(context.TODO(), *c.group)
	} else {
		group, err = client.NewGroup(*c.group, client.WithApplication(*c.app))
	}

	if err != nil {
		return err
	}

	_map, err := group.GetMap(context.TODO(), *c._name)
	if err != nil {
		return err
	}

	value, err := _map.Get(context.Background(), *c.key)
	if err != nil {
		return err
	}

	if value != nil {
		println(fmt.Sprintf("%v", value.Value))
	}
	return nil
}

func getMapPutCommand() command {
	return &mapPutCommand{
		flag: flag.NewFlagSet("put", flag.ExitOnError),
	}
}

type mapPutCommand struct {
	command
	flag       *flag.FlagSet
	controller *string
	namespace  *string
	group      *string
	app        *string
	_name      *string
	key        *string
	value      *string
}

func (c *mapPutCommand) name() string {
	return "put"
}

func (c *mapPutCommand) init() error {
	c.controller = c.flag.String("controller", "", "the controller for the group")
	c.namespace = c.flag.String("namespace", "default", "the partition group namespace")
	c.group = c.flag.String("group", "", "the partition group name")
	c.app = c.flag.String("app", "default", "the application name")
	c._name = c.flag.String("name", "", "the map name")
	c.key = c.flag.String("key", "", "the key to set")
	c.value = c.flag.String("value", "", "the value to set")
	return nil
}

func (c *mapPutCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *mapPutCommand) execute() error {
	var group *client.PartitionGroup
	var err error
	if *c.controller != "" {
		cl, err := client.NewClient(*c.controller, client.WithApplication(*c.app), client.WithNamespace(*c.namespace))
		if err != nil {
			return err
		}
		group, err = cl.GetGroup(context.TODO(), *c.group)
	} else {
		group, err = client.NewGroup(*c.group, client.WithApplication(*c.app))
	}

	if err != nil {
		return err
	}

	_map, err := group.GetMap(context.TODO(), *c._name)
	if err != nil {
		return err
	}

	value, err := _map.Put(context.Background(), *c.key, []byte(*c.value))
	if err != nil {
		return err
	}

	if value != nil {
		println(fmt.Sprintf("%v", value.Value))
	}
	return nil
}
