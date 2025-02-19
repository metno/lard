package db

import "slices"

type BaseConfig struct {
	Path         string   `arg:"-p" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables       []string `arg:"-t" help:"Optional space separated list of table names"`
	Stations     []string `arg:"-s" help:"Optional space separated list of stations IDs"`
	Elements     []string `arg:"-e" help:"Optional space separated list of element codes"`
	SkipTables   []string `arg:"-t" help:"Optional space separated list of table names to skip"`
	SkipStations []string `arg:"-s" help:"Optional space separated list of stations IDs to skip"`
	SkipElements []string `arg:"-e" help:"Optional space separated list of element codes to skip"`
}

func (c *BaseConfig) ShouldProcessTable(table string) bool {
	should := true
	if c.Tables != nil {
		should = should && slices.Contains(c.Tables, table)
	}
	if c.SkipTables != nil {
		should = should && !slices.Contains(c.SkipTables, table)
	}
	return should
}

func (c *BaseConfig) ShouldProcessStation(table string) bool {
	should := true
	if c.Stations != nil {
		should = should && slices.Contains(c.Stations, table)
	}
	if c.SkipStations != nil {
		should = should && !slices.Contains(c.SkipStations, table)
	}
	return should
}

func (c *BaseConfig) ShouldProcessElement(table string) bool {
	should := true
	if c.Elements != nil {
		should = should && slices.Contains(c.Elements, table)
	}
	if c.SkipElements != nil {
		should = should && !slices.Contains(c.SkipElements, table)
	}
	return should
}
