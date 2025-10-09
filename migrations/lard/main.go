package lard

import (
	"reflect"
	"time"
)

const LARD_OPEN_ENV_VAR string = "LARD_OPEN_CONN_STRING"
const LARD_RESTRICTED_ENV_VAR string = "LARD_RESTRICTED_CONN_STRING"

const TEST_CONN_STRING_OPEN string = "host=localhost user=postgres dbname=lard password=postgres"
const TEST_CONN_STRING_RESTRICTED string = "host=localhost user=postgres dbname=lard_restricted password=postgres"

// Number of columns in the nonscalar_data table
var NONSCALAR_DATA_COLUMNS int = reflect.ValueOf(TextObs{}).NumField()

// Number of columns in the legacy.data table
var LEGACY_DATA_COLUMNS int = reflect.ValueOf(LegacyObs{}).NumField()

// Struct mimicking the `public.nonscalar_data` table
type TextObs struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Observation data that cannot be represented as a float, therefore stored as a string
	Text *string
}

func (o *TextObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Text}
}

type LegacyObs struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Raw observation value
	Original *float64
	// Corrected value after QC tests
	Corrected *float64
	// QualityCode code of the observation
	// Not all observations have one
	QualityCode *int32
	// Flag encoding quality control status
	Controlinfo *string
	// Flag encoding quality control status
	Useinfo *string
	// Number of tests that failed?
	Cfailed *string
}

func (o *LegacyObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Original, o.Corrected, o.QualityCode, o.Controlinfo, o.Useinfo, o.Cfailed}
}
