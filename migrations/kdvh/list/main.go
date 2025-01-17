package list

import (
	"fmt"
	"slices"

	"migrate/kdvh/dump"
)

type Config struct{}

func (config *Config) Execute() {
	fmt.Println("Available tables in KDVH:")

	tables := dump.InitDump()

	var names []string
	for _, table := range tables {
		names = append(names, table.TableName)
	}

	slices.Sort(names)
	for _, name := range names {
		fmt.Println("    -", name)
	}
}
