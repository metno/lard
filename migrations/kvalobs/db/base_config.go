package db

import (
	"fmt"
	"slices"
	"time"
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

func (config *BaseConfig) ShouldProcessStation(stnr int64) bool {
	var result bool
	station := int32(stnr)

	if config.Stations != nil {
		result = slices.Contains(config.Stations, station)
	}

	if config.SkipStations != nil {
		result = !slices.Contains(config.SkipStations, station)
	}

	return result
}

func (config *BaseConfig) ShouldProcessLabel(label *Label) bool {
	result := true
	if config.ParamIds != nil {
		result = result && slices.Contains(config.ParamIds, label.ParamID)
	}
	if config.TypeIds != nil {
		result = result && slices.Contains(config.TypeIds, label.TypeID)
	}
	if config.Sensors != nil {
		if label.Sensor == nil {
			return false
		}
		result = result && slices.Contains(config.Sensors, *label.Sensor)
	}
	if config.Levels != nil {
		if label.Level == nil {
			return false
		}
		result = result && slices.Contains(config.Levels, *label.Level)
	}

	if config.SkipParamIds != nil {
		result = result && !slices.Contains(config.SkipParamIds, label.ParamID)
	}
	if config.SkipTypeIds != nil {
		result = result && !slices.Contains(config.SkipTypeIds, label.TypeID)
	}
	if config.SkipSensors != nil {
		if label.Sensor != nil {
			result = result && !slices.Contains(config.SkipSensors, *label.Sensor)
		}
	}
	if config.SkipLevels != nil {
		if label.Level != nil {
			result = result && !slices.Contains(config.SkipLevels, *label.Level)
		}
	}
	return result
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
