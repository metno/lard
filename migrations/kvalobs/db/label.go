package db

import (
	"errors"
	"fmt"
	"migrate/lard"
	"migrate/utils"
	"slices"
	"strconv"
	"strings"
)

var METAR_CLOUD_TYPES []int32 = []int32{2751, 2752, 2753, 2754}
var SPECIAL_CLOUD_TYPES []int32 = []int32{305, 306, 307, 308}

// Kvalobs specific label
type Label struct {
	StationID int32 `db:"stationid"`
	ParamID   int32 `db:"paramid"`
	TypeID    int32 `db:"typeid"`
	// These two are not present in the `text_data` tabl
	Sensor *int32 `db:"sensor"` // bpchar(1) in `data` table
	Level  *int32 `db:"level"`
	// LogStr string
}

func (l *Label) IsMetarCloudType() bool {
	return slices.Contains(METAR_CLOUD_TYPES, l.ParamID)
}

func (l *Label) IsSpecialCloudType() bool {
	return slices.Contains(SPECIAL_CLOUD_TYPES, l.ParamID)
}

func (l *Label) sensorLevelString() (string, string) {
	var sensor, level string
	if l.Sensor != nil {
		sensor = fmt.Sprint(*l.Sensor)
	}
	if l.Level != nil {
		level = fmt.Sprint(*l.Level)
	}
	return sensor, level
}

func (l *Label) ToFilename() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf("%v_%v_%v_%v_%v.csv", l.StationID, l.ParamID, l.TypeID, sensor, level)
}

func (l *Label) LogStr() string {
	sensor, level := l.sensorLevelString()
	return fmt.Sprintf(
		"[%v|%v|%v|%v|%v]: ",
		l.StationID, l.ParamID, l.TypeID, sensor, level,
	)
}

func (l *Label) ToLard() *lard.Label {
	label := lard.Label(*l)
	return &label
}

func parseFilenameFields(s *string) (*int32, error) {
	if *s == "" {
		return nil, nil
	}
	res, err := strconv.ParseInt(*s, 10, 32)
	if err != nil {
		return nil, err
	}
	out := int32(res)
	return &out, nil
}

// Deserialize filename to LardLabel
func LabelFromFilename(filename string) (*Label, error) {
	name := strings.TrimSuffix(filename, ".csv")

	fields := strings.Split(name, "_")
	if len(fields) != 5 {
		return nil, errors.New("Wrong number of fields in file name: " + filename)
	}

	ptrs := make([]*string, len(fields))
	for i := range ptrs {
		ptrs[i] = &fields[i]
	}

	converted, err := utils.TryMap(ptrs, parseFilenameFields)
	if err != nil {
		return nil, err
	}

	return &Label{
		StationID: *converted[0],
		ParamID:   *converted[1],
		TypeID:    *converted[2],
		Sensor:    converted[3],
		Level:     converted[4],
	}, nil
}
