package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	logprotocol "github.com/atomix/atomix-go-client/pkg/client/protocol/log"
	"github.com/atomix/atomix-go-client/pkg/client/protocol/raft"
	"google.golang.org/grpc"
	"log"
	"os"
)

func main() {
	host := flag.String("host", "localhost", "the controller host")
	port := flag.Int("port", 5679, "the controller port")
	application := flag.String("application", "", "the client application")
	namespace := flag.String("namespace", "", "the client namespace")

	commands := getCommands()
	for _, cmd := range commands {
		if err := cmd.init(); err != nil {
			log.Fatal("Failed to initialize command parser", err)
		}
	}

	flag.Parse()

	c, err := client.NewClient(*application, *namespace, fmt.Sprintf("%s:%d", *host, *port), grpc.WithInsecure())
	if err != nil {
		log.Fatal("failed to establish client connection", err)
	}

	i := 1
	for i < len(os.Args) {
		arg := os.Args[i]
		if arg == "-host" || arg == "-port" || arg == "-application" || arg == "-namespace" {
			i += 2
		} else {
			break
		}
	}

	if len(os.Args) <= i {
		log.Fatal("No command specified")
	}

	name := os.Args[i]
	for _, cmd := range commands {
		if cmd.name() == name {
			if err := cmd.parse(os.Args[i+1:]); err != nil {
				log.Fatal("Failed to parse command", err)
			}
			if err := cmd.execute(c); err != nil {
				log.Fatal("Failed to execute command", err)
			}
		}
	}
}

func getCommands() []command {
	return []command{
		newCreateCommand(),
		newGetCommand(),
		newDeleteCommand(),
		newMapCommand(),
	}
}

type command interface {
	name() string
	init() error
	parse(args []string) error
	execute(cl *client.Client) error
}

func newCreateCommand() command {
	return &createCommand{
		flag: flag.NewFlagSet("create", flag.ExitOnError),
	}
}

type createCommand struct {
	command
	flag          *flag.FlagSet
	group         *string
	partitions    *int
	partitionSize *int
	protocol      *string
}

func (c *createCommand) name() string {
	return "create"
}

func (c *createCommand) init() error {
	c.group = c.flag.String("group", "", "the partition group name")
	c.partitions = c.flag.Int("partitions", 1, "the number of partitions to create")
	c.partitionSize = c.flag.Int("partitionSize", 1, "the partition size")
	c.protocol = c.flag.String("protocol", "raft", "the partition protocol")
	return nil
}

func (c *createCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *createCommand) execute(cl *client.Client) error {
	var p protocol.Protocol
	switch *c.protocol {
	case "raft":
		p = &raft.Protocol{}
	case "log":
		p = &logprotocol.Protocol{}
	}
	_, err := cl.CreatePartitionGroup(*c.group, *c.partitions, *c.partitionSize, p)
	return err
}

func newGetCommand() command {
	return &getCommand{
		flag: flag.NewFlagSet("get", flag.ExitOnError),
	}
}

type getCommand struct {
	command
	flag  *flag.FlagSet
	group *string
}

func (c *getCommand) name() string {
	return "get"
}

func (c *getCommand) init() error {
	c.group = c.flag.String("group", "", "the partition group name")
	return nil
}

func (c *getCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *getCommand) execute(cl *client.Client) error {
	group, err := cl.GetPartitionGroup(*c.group)
	println(fmt.Sprintf("%v", group))
	return err
}

func newDeleteCommand() command {
	return &deleteCommand{
		flag: flag.NewFlagSet("delete", flag.ExitOnError),
	}
}

type deleteCommand struct {
	command
	flag  *flag.FlagSet
	group *string
}

func (c *deleteCommand) name() string {
	return "delete"
}

func (c *deleteCommand) init() error {
	c.group = c.flag.String("group", "", "the partition group name")
	return nil
}

func (c *deleteCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *deleteCommand) execute(cl *client.Client) error {
	return cl.DeletePartitionGroup(*c.group)
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

func (c *mapCommand) execute(cl *client.Client) error {
	return c.action.execute(cl)
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
	flag  *flag.FlagSet
	group *string
	_name *string
	key   *string
}

func (c *mapGetCommand) name() string {
	return "get"
}

func (c *mapGetCommand) init() error {
	c.group = c.flag.String("group", "", "the partition group name")
	c._name = c.flag.String("name", "", "the map name")
	c.key = c.flag.String("key", "", "the key to get")
	return nil
}

func (c *mapGetCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *mapGetCommand) execute(cl *client.Client) error {
	group, err := cl.GetPartitionGroup(*c.group)
	if err != nil {
		return err
	}

	_map, err := group.NewMap(*c._name)
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
	flag  *flag.FlagSet
	group *string
	_name *string
	key   *string
	value *string
}

func (c *mapPutCommand) name() string {
	return "put"
}

func (c *mapPutCommand) init() error {
	c.group = c.flag.String("group", "", "the partition group name")
	c._name = c.flag.String("name", "", "the map name")
	c.key = c.flag.String("key", "", "the key to set")
	c.value = c.flag.String("value", "", "the value to set")
	return nil
}

func (c *mapPutCommand) parse(args []string) error {
	return c.flag.Parse(args)
}

func (c *mapPutCommand) execute(cl *client.Client) error {
	group, err := cl.GetPartitionGroup(*c.group)
	if err != nil {
		return err
	}

	_map, err := group.NewMap(*c._name)
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
