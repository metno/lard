package lard

import "time"

const LARD_OPEN_ENV_VAR string = "LARD_OPEN_CONN_STRING"
const LARD_RESTRICTED_ENV_VAR string = "LARD_RESTRICTED_CONN_STRING"

const TEST_CONN_STRING_OPEN string = "host=localhost user=postgres dbname=lard password=postgres"
const TEST_CONN_STRING_RESTRICTED string = "host=localhost user=postgres dbname=lard_restricted password=postgres"

// Struct mimicking the `public.data` table
// NOTE: this does not have the `qc_usable` field
type DataObs struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Observation data formatted as a single precision floating point number
	Data *float64
}

func (o *DataObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Data}
}

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

type LegacyData struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
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

func (o *LegacyData) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Corrected, o.QualityCode, o.Controlinfo, o.Useinfo, o.Cfailed}
}
