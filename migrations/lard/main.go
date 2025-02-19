package lard

import "time"

const LARD_ENV_VAR string = "LARD_CONN_STRING"

// Struct mimicking the `public.data` table
// TODO: add qc_usable field? Derived from LegacyData.QualityCode?
type DataObs struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Observation data formatted as a single precision floating point number
	Data *float64
	// Whether the observation passed quality control
	// This is derived from the kvalobs flags (see the `isQcUsable` function)
	QcUsable bool
}

func (o *DataObs) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Data, o.QcUsable}
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
}

func (o *LegacyData) ToRow() []any {
	return []any{o.Id, o.Obstime, o.Corrected, o.QualityCode}
}

// Struct mimicking the `flags.kvdata` table
type LegacyFlag struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Flag encoding quality control status
	Controlinfo *string
	// Flag encoding quality control status
	Useinfo *string
	// Number of tests that failed?
	Cfailed *string
}

func (o *LegacyFlag) ToRow() []any {
	// "timeseries", "obstime", "corrected","controlinfo", "useinfo", "cfailed"
	return []any{o.Id, o.Obstime, o.Controlinfo, o.Useinfo, o.Cfailed}
}
