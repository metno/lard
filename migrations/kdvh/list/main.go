package list

import (
	"fmt"
	"slices"

	"migrate/kdvh/dump"
	port "migrate/kdvh/import"
)

type Config struct{}

func (config *Config) Execute() {
	printTablesToDump()
	printTablesToImport()
}

func printTablesToDump() {
	fmt.Println("Available KDVH tables to dump:")
	tables := dump.InitDumpTables()

	var names []string
	for _, table := range tables {
		names = append(names, table.TableName)
	}

	printTables(names)
}

func printTablesToImport() {
	fmt.Println("Available KDVH tables to import:")

	var names []string
	for name := range port.IMPORT_TABLES {
		names = append(names, name)
	}

	printTables(names)
}

func printTables(tables []string) {
	slices.Sort(tables)
	for _, table := range tables {
		fmt.Println("    -", table)
	}
}
