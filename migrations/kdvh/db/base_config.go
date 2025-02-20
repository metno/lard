package db

import (
	"migrate/utils"
	"slices"
)

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
	return utils.IsNilOrContains(c.Tables, table) &&
		!slices.Contains(c.SkipTables, table)
}

func (c *BaseConfig) ShouldProcessStation(station string) bool {
	return utils.IsNilOrContains(c.Stations, station) &&
		!slices.Contains(c.SkipStations, station)
}

func (c *BaseConfig) ShouldProcessElement(element string) bool {
	return utils.IsNilOrContains(c.Elements, element) &&
		!slices.Contains(c.SkipElements, element)
}
