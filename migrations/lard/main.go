package lard

import "time"

const LARD_ENV_VAR string = "LARD_CONN_STRING"

// Struct mimicking the `public.data` table
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

// Struct mimicking the `flags.kvdata` table
type Flag struct {
	// Timeseries ID
	Id int64
	// Time of observation
	Obstime time.Time
	// Original value before QC tests
	Original *float64
	// Corrected value after QC tests
	Corrected *float64
	// Flag encoding quality control status
	Controlinfo *string
	// Flag encoding quality control status
	Useinfo *string
	// Number of tests that failed?
	Cfailed *string
}

func (o *Flag) ToRow() []any {
	// "timeseries", "obstime", "corrected","controlinfo", "useinfo", "cfailed"
	return []any{o.Id, o.Obstime, o.Original, o.Corrected, o.Controlinfo, o.Useinfo, o.Cfailed}
}
