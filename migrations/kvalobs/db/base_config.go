package db

import (
	"fmt"
	"slices"
	"time"

	"migrate/utils"
)

// TODO: should we use this one as default or process all times
// TODO: it looks like histkvalobs has data only starting from 2023-06-01?
var FROMTIME time.Time = time.Date(2006, 01, 01, 00, 00, 00, 00, time.UTC)

type BaseConfig struct {
	Path         string  `arg:"-p" default:"./dumps" help:"Location the dumped data will be stored in"`
	Database     string  `arg:"--db" help:"Which database to process, all by default. Choices: ['kvalobs', 'histkvalobs']"`
	Table        string  `help:"Which table to process, all by default. Choices: ['data', 'text_data']"`
	Stations     []int32 `help:"Optional space separated list of station numbers"`
	TypeIds      []int32 `help:"Optional space separated list of type IDs"`
	ParamIds     []int32 `help:"Optional space separated list of param IDs"`
	Sensors      []int32 `help:"Optional space separated list of sensors"`
	Levels       []int32 `help:"Optional space separated list of levels"`
	SkipStations []int32 `help:"Optional space separated list of station numbers to skip"`
	SkipTypeIds  []int32 `help:"Optional space separated list of type IDs to skip"`
	SkipParamIds []int32 `help:"Optional space separated list of param IDs to skip"`
	SkipSensors  []int32 `help:"Optional space separated list of sensors to skip"`
	SkipLevels   []int32 `help:"Optional space separated list of levels to skip"`
}

func (c *BaseConfig) ShouldProcessStation(station int32) bool {
	return utils.IsNilOrContains(c.Stations, station) &&
		!slices.Contains(c.SkipStations, station)
}

func (c *BaseConfig) ShouldProcessLabel(label *Label) bool {
	return (utils.IsNilOrContains(c.ParamIds, label.ParamID) &&
		utils.IsNilOrContains(c.TypeIds, label.ParamID) &&
		utils.IsNilOrContainsPtr(c.Sensors, label.Sensor) &&
		utils.IsNilOrContainsPtr(c.Levels, label.Level) &&
		!slices.Contains(c.SkipParamIds, label.ParamID) &&
		!slices.Contains(c.SkipTypeIds, label.TypeID) &&
		!utils.ContainsPtr(c.SkipSensors, label.Sensor) &&
		!utils.ContainsPtr(c.SkipLevels, label.Level))

}

func (config *BaseConfig) CheckSpelling() error {
	switch config.Database {
	case "", KvDbName, HistDbName:
	default:
		return fmt.Errorf("The '--db' flag expects either 'kvalobs' or 'histkvalobs' as input, got '%s'", config.Database)
	}

	switch config.Table {
	case "", DataTableName, TextTableName:
	default:
		return fmt.Errorf("The '--table' flag expects either 'data' or 'text_data' as input, got '%s'", config.Table)
	}

	return nil
}
