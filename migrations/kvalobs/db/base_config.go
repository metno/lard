package db

import (
	"fmt"
	"slices"

	"migrate/utils"
)

// var KVALOBS_START_TIME time.Time = time.Date(2006, 01, 01, 00, 00, 00, 00, time.UTC)

type BaseConfig struct {
	Path         string  `arg:"-p" help:"Location the dumped data will be stored in"`
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
	Test         bool    `arg:"-"` // Used for testing (mostly to avoid logging to files)
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

func (config *BaseConfig) CheckTableSpelling() error {
	switch config.Table {
	case "", DataTableName, TextTableName:
	default:
		return fmt.Errorf("The '--table' flag expects either 'data' or 'text_data' as input, got '%s'", config.Table)
	}

	return nil
}
