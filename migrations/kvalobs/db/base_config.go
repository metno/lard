package db

import (
	"time"

	"migrate/utils"
)

// TODO: should we use this one as default or process all times
// TODO: it looks like histkvalobs has data only starting from 2023-06-01?
var FROMTIME time.Time = time.Date(2006, 01, 01, 00, 00, 00, 00, time.UTC)

type BaseConfig struct {
	Path     string  `arg:"-p" default:"./dumps" help:"Location the dumped data will be stored in"`
	Database string  `arg:"--db" help:"Which database to process, all by default. Choices: ['kvalobs', 'histkvalobs']"`
	Table    string  `help:"Which table to process, all by default. Choices: ['data', 'text_data']"`
	Stations []int32 `help:"Optional space separated list of station numbers"`
	TypeIds  []int32 `help:"Optional space separated list of type IDs"`
	ParamIds []int32 `help:"Optional space separated list of param IDs"`
	Sensors  []int32 `help:"Optional space separated list of sensors"`
	Levels   []int32 `help:"Optional space separated list of levels"`
}

func (config *BaseConfig) SetPath(path string) {
	config.Path = path
}

func (config *BaseConfig) ShouldProcessLabel(label *Label) bool {
	return utils.IsNilOrContains(config.ParamIds, label.ParamID) &&
		// utils.IsEmptyOrContains(config.Stations, label.StationID) &&
		utils.IsNilOrContains(config.TypeIds, label.TypeID) &&
		// TODO: these two should never be null anyway?
		utils.IsNilOrContainsPtr(config.Sensors, label.Sensor) &&
		utils.IsNilOrContainsPtr(config.Levels, label.Level)
}
