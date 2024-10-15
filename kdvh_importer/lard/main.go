package lard

import "time"

// TODO: define the schema we want to export here
// TODO: are there IDs from the systems we are migrating from that we should store here?
// Might be useful if things go wrong
// And probably we should use nullable types?
// type Obs struct {
// 	// Unique timeseries identifier
// 	ID int32
// 	// Time of observation
// 	ObsTime time.Time
// 	// Observation data formatted as a double precision floating point
// 	Data *float64
// 	// Observation data that cannot be represented as a float
// 	NonScalarData []byte
// 	// Flag encoding quality control status
// 	KVFlagControlInfo []byte
// 	// Flag encoding quality control status
// 	KVFlagUseInfo []byte
// 	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
// 	// KDVHFlag []byte
// 	// Comma separated value listing checks that failed during quality control
// 	// KVCheckFailed string
// }

// TODO: define the schema we want to export here
// TODO: are there IDs from the systems we are migrating from that we should store here?
// Might be useful if things go wrong
// And probably we should use nullable types?
type Obs struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Observation data formatted as a double precision floating point
	Data float64
}

type NonScalarObs struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Observation data that cannot be represented as a float
	Data []byte
}

// KDVH specific
type Flags struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Flag encoding quality control status
	KVFlagControlInfo []byte
	// Flag encoding quality control status
	KVFlagUseInfo []byte
	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
	// KDVHFlag []byte
	// Comma separated value listing checks that failed during quality control
	// KVCheckFailed string
}

type LardData interface {
	isData() bool
}

func (o *Obs) isData() bool {
	return true
}

func (o *NonScalarObs) isData() bool {
	return true
}
