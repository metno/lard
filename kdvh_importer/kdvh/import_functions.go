package kdvh

import (
	"errors"
	"strconv"

	"github.com/rickb777/period"
)

// TODO: maybe we won't even need to use these flags in the end??

// In kvalobs a flag is a 16 char string containg QC information about the observation:

// controlinfo flag:
// Missing numbers are reserved
//
// 0 Control level (not used)
//
// 1 Range check
//  0. Not checked
//  1. Pass
//  2. Higher than HIGH
//  3. Lower than LOW
//  4. Higher than HIGHER
//  5. Lower that LOWER
//  6. Rejected, above HIGHEST or below LOWEST
//
// 2 Formal consistency check
//  0. Not checked
//  1. Pass
//  2. Inconsistency, but not an error with the relevant parameter, no correction
//  3. Inconsistency at the observation time, but not possible to determine which parameter, no correction
//  4. Inconsistency at earliar/later observation times, but not possible to determine which parameter, no correction
//  6. Inconsistency at the observation time, probably error with the relevant parameter, no correction
//  7. Inconsistency at earliar/later observation times, probably error with relevant parameter, no correction
//  8. Inconsistency, a parameter is missing, no correction
//     A. Inconsistency at the observation time, corrected automatically
//     B. Inconsistency at earliar/later observation times, corrected automatically
//     D. Rejected
//
// 3 Jump check (step, dip, freeze, drift)
//  0. Not checked
//  1. Pass
//  2. Change higher than test value, no correction
//  3. No change in measured value (freeze check did not pass?), no correction
//  4. Suspected error in freeze check, no error in dip check (??), no correction
//  5. Suspected error in dip check, no error in freeze check (??), no correction
//  7. Observed drift, no correction
//  9. Change higher than test value, corrected automatically
//     A. Freeze check did not pass, corrected automatically
//
// 4 Prognostic check
//  0. Not checked
//  1. Pass
//  2. Deviation from model higher than HIGH
//  3. Deviation from model lower than LOW
//  4. Deviation from model higher than HIGHER
//  5. Deviation from model lower that LOWER
//  6. Rejected, deviation from model above HIGHEST or below LOWEST
//
// 5 Message check (for moving stations)
//  0. Not checked
//  1. Pass
//  3. Suspicious message, no correction
//  4. Suspicious message, corrected automatically
//  6. Rejected
//
// 6 Missing observations
//  0. Original and corrected values exist
//  1. Original value missing, but corrected value exists
//  2. Corrected value missing, orginal value discarded
//  3. Original and corrected values missing
//
// 7 Timeseries fitting
//  0. Not checked
//  1. Interpolated with good fitness
//  2. Interpolated with unsure fitness
//  3. Intepolation not suitable
//
// 8 Weather analysis
//  0. Not checked
//  1. Pass
//  2. Suspicious value, not corrected
//  3. Suspicious value, corrected automatically
//
// 9 Statistical check
//  0. Not checked
//  1. Pass
//  2. Suspicious value, not corrected
//
// 10 Climatological consistency check
//  0. Not checked
//  1. Pass
//  2. Climatologically questionable, but not an error with the relevant parameter, no correction
//  3. Climatologically questionable at the observation time, but not possible to determine which parameter, no correction
//  4. Climatologically questionable at earliar/later observation times, but not possible to determine which parameter, no correction
//  6. Climatologically questionable at the observation time, probably error with the relevant parameter, no correction
//  7. Climatologically questionable at earliar/later observation times, probably error with relevant parameter, no correction
//     A. Inconsistency at the observation time, corrected automatically
//     B. Inconsistency at earliar/later observation times, corrected automatically
//     D. Rejected
//
// 11 Climatological check
//  0. Not checked
//  1. Pass
//  2. Suspicious value, not corrected
//  3. Suspicious value, corrected automatically
//
// 12 Distribution check of accumulated parameters (especially for precipitation)
//  0. Not checked
//  1. Not an accumulated value
//  2. Observation outside accumulated parameter range
//  3. Abnormal observation (??)
//  6. Accumulation calculated from numerical model
//  7. Accumulation calculated from weather analysis
//     A. Accumulation calculated with 'steady rainfall' method
//     B. Accumulation calculated with 'uneven rainfall' method
//
// 13 Prequalification (certain pairs of 'stationid' and 'paramid' can be discarded)
//  0. Not checked
//  5. Value is missing
//  6. Rejected, invalid original value
//  7. Rejected, original value is noisy
//
// 14 Combination check
//  0. Not checked
//  1. Pass
//  2. Outside test limit value, but no jumps detected and inside numerical model tolerance
//  9. Rejected. Outside test limit value, no jumps detected but outside numerical model tolerance
//     A. Rejected. Outside test limit value, jumps detected but inside numerical model tolerance
//     B. Rejected. Outside test limit value, jumps detected and outside numerical model tolerance
//
// 15 Manual quality control
//  0. Not checked
//  1. Pass
//  2. Probably OK
//  5. Value manually interpolated
//  6. Value manually assigned
//  7. Value manually corrected
//     A. Manually rejected
const (
	EMPTY                               = "00000" + "00000000000"
	EXPECTED_VALUE_MISSING              = "00000" + "03000000000"
	VALUE_REMOVED_BY_QC                 = "00000" + "02000000000"
	MANUALLY_INTERPOLATED               = "00000" + "00400000005"
	CORRECTED_WITH_MANUAL_INTERPOLATION = "00000" + "01000000005"
	INVALID                             = "99999" + "00900000000"
)

// useinfo flag:
// Missing numbers are reserved
//
// 0 Control levels passed
//  1. Completed QC1, QC2 and HQC
//  2. Completed QC2 and HQC
//  3. Completed QC1 and HQC
//  4. Completed HQC
//  5. Completed QC1 and QC2
//  6. Completed QC2
//  7. Completed QC1
//  9. Missing information
//
// 1 Deviation from norm (mean?)
//  0. Observation time and period are okay
//  1. Observation time deviates from norm
//  2. Observation period is shorter than norm
//  3. Observation perios is longer than norm
//  4. Observation time deviates from norm, and period is shorter than norm
//  5. Observation time deviates from norm, and period is longer than norm
//  8. Missing value
//  9. Missing status information
//
// 2 Quality level of orignal value
//  0. Value is okay
//  1. Value is suspicious (probably correct)
//  2. Value is suspicious (probably wrong)
//  3. Value is wrong
//  9. Missing quality information
//
// 3 Treatment of original value
//  0. Unchanged
//  1. Manually corrected
//  2. Manually interpolated
//  3. Automatically corrected
//  4. Automatically interpolated
//  5. Manually derived from accumulated value
//  6. Automatically derived from accumulated value
//  8. Rejected
//  9. Missing information
//
// 4 Most import check result (?)
//  0. Original value is okay
//  1. Range check
//  2. Consistency check
//  3. Jump check
//  4. Consistency check in relation with earlier/later observations
//  5. Prognostic check based on observation data
//  6. Prognostic check based on Timeseries
//  7. Prognostic check based on model data
//  8. Prognostic check based on statistics
//  9. Missing information
//
// 7 Delay information
//  0. Observation carried out and reported at the right time
//  1. Observation carried out early and reported at the right time
//  2. Observation carried out late and reported at the right time
//  3. Observation reported early
//  4. Observation reported late
//  5. Observation carried out early and reported late
//  6. Observation carried out late and reported late
//  9. Missing information
//
// 8-9 Hexadecimal value of the observation confidence level
//
// 13-14 HQC operator digits
//
// 15 Hexadecimal digit with number of tests that did not pass (returned a result?)
const (
	// This somehow means the observation is missing?
	// Because from the docs the 9 should be changed by kvalobs when the observation is inserted into the database
	DELAY_NOT_CONSIDERED = "00900000000"
	DELAY_IN_TIME        = "00000000000"

	COMPLETED_HQC                = "40000" + DELAY_NOT_CONSIDERED
	DIURNAL_INTERPOLATED_USEINFO = "48925" + "00900000000"
)

// TODO: should implement tests for these functions

func makeDataPage(obs Obs) (ObsLard, error) {
	var useinfo, controlinfo string
	var nonscalar *string
	var valPtr *float64

	val, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		// Check if data is missing
		if obs.Data == "" {
			controlinfo = EXPECTED_VALUE_MISSING
		} else {
			controlinfo = EMPTY
			nonscalar = &obs.Data
		}
	} else {
		controlinfo = EMPTY
		valPtr = &val
	}

	// set useinfo
	if obs.flagsAreInvalid() {
		useinfo = INVALID
	} else {
		// TODO: should this be DELAY_NOT_CONSIDERED or DELAY_IN_TIME
		useinfo = obs.Flags + DELAY_NOT_CONSIDERED
	}

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              valPtr,
		NonScalarData:     nonscalar,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
	}, nil
}

// modify obstimes to always use totime
func makeDataPageProduct(obs Obs) (ObsLard, error) {
	obsLard, err := makeDataPage(obs)
	if !obs.Offset.IsZero() {
		if temp, ok := obs.Offset.AddTo(obsLard.ObsTime); ok {
			obsLard.ObsTime = temp
		}
	}
	return obsLard, err
}

// TODO: it would be nice to have a definition of these flag values
// write flags correctly for T_EDATA
func makeDataPageEdata(obs Obs) (ObsLard, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	if obs.flagsAreInvalid() {
		useinfo = INVALID
	} else {
		useinfo = obs.Flags + DELAY_NOT_CONSIDERED
	}

	val, err := strconv.ParseFloat(obs.Data, 64)
	// Check nullData
	if err != nil {
		switch obs.Flags {
		case "70381", "70389", "90989":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// includes "70000", "70101", "99999"
			controlinfo = EXPECTED_VALUE_MISSING
		}
	} else {
		valPtr = &val
		controlinfo = EMPTY
	}

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              valPtr,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
	}, nil
}

func makeDataPagePdata(obs Obs) (ObsLard, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	if obs.flagsAreInvalid() {
		useinfo = INVALID
	} else {
		// useinfo = obs.Flags "00000"+ "00000000000"
		useinfo = obs.Flags + DELAY_NOT_CONSIDERED
	}

	val, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		switch obs.Flags {
		case "70381", "71381", "50383", "20389", "30389", "40389":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// "00000", "10000", "10319", "30000", "30319",
			// "40000", "40929", "48929", "48999", "50000",
			// "50205", "60000", "70000", "70103", "70203",
			// "71000", "71203", "90909", "99999"
			controlinfo = EXPECTED_VALUE_MISSING
		}
	} else {
		// TODO: make sure these flags are correct and move them in the const enum
		valPtr = &val
		switch obs.Flags {
		case "10319", "30319", "40319", "10329":
			// TODO: position 6 range should only be [0-3]
			controlinfo = "00000" + "04000000005"
		case "99319", "70381", "71381", "50383", "20389", "30389", "40389":
			// TODO: position 6 range should only be [0-3]
			controlinfo = "00000" + "04000000000"
		case "40929":
			controlinfo = "00000" + "00000000005"
		case "48929", "48999":
			controlinfo = "00000" + "01000000005"
		default:
			// "90909", "71000", "71203", "99999"
			controlinfo = EMPTY
		}
	}

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              valPtr,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
	}, nil
}

func makeDataPageNdata(obs Obs) (ObsLard, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	if obs.flagsAreInvalid() {
		useinfo = INVALID
	} else {
		// TODO: why would this
		useinfo = obs.Flags + DELAY_NOT_CONSIDERED
	}

	floatval, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		// null data

		switch obs.Flags {
		case "70389":
			controlinfo = VALUE_REMOVED_BY_QC
		default:
			// "30319", "38929", "40000", "40100", "40315"
			// "40319", "43325", "48325", "49225", "49915"
			// "70000", "70204", "71000", "73309", "78937"
			// "90909", "93399", "98999", "99999"
			controlinfo = EXPECTED_VALUE_MISSING
		}
	} else {
		switch obs.Flags {
		case "30319", "40315", "40319":
			controlinfo = "00000" + "04000000005"
		case "38929":
			controlinfo = "00000" + "01000000005"
		case "40000", "40100":
			controlinfo = "00000" + "00000000001"
		case "43325":
			controlinfo = "00000" + "04000000006"
		case "48325":
			controlinfo = "00000" + "01000000006"
		case "49225", "49915":
			controlinfo = "00000" + "00000000005"
		case "70389", "73309", "93399":
			controlinfo = "00000" + "04000000000"
		case "78937", "98999":
			controlinfo = "00000" + "01000000000"
		default:
			// "70000", "70204", "71000", "90909", "99999"
			controlinfo = EMPTY
		}
		valPtr = &floatval
	}

	// switch obs.Flags {
	// default:
	// 	if obs.flagsAreInvalid() {
	// 		useinfo = INVALID
	// 	} else {
	// 		// TODO: why would this be 00000000000 and not 00900000000?
	// 		useinfo = obs.Flags + "00000000000"
	// 	}
	// }

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              valPtr,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
	}, nil
}

func makeDataPageVdata(obs Obs) (ObsLard, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	// set useinfo based on time
	if h := obs.ObsTime.Hour(); h == 0 || h == 6 || h == 12 || h == 18 {
		useinfo = COMPLETED_HQC
	} else {
		useinfo = INVALID
	}

	// set data and controlinfo
	floatval, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		controlinfo = EXPECTED_VALUE_MISSING
	} else {
		// super special treatment clause of T_VDATA.OT_24, so it will be the same as in kvalobs
		if obs.ElemCode == "OT_24" {
			// add custom offset, because OT_24 in KDVH has been treated differently than OT_24 in kvalobs
			offset, err := period.Parse("PT18H") // fromtime_offset -PT6H, timespan P1D
			if err != nil {
				return ObsLard{}, errors.New("could not parse period")
			}
			temp, ok := offset.AddTo(obs.ObsTime)
			if !ok {
				return ObsLard{}, errors.New("could not add period")
			}

			obs.ObsTime = temp
			// convert from hours to minutes
			floatval *= 60
		}
		valPtr = &floatval
		controlinfo = EMPTY
	}

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              valPtr,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
	}, nil
}

func makeDataPageDiurnalInterpolated(obs Obs) (ObsLard, error) {
	val, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		return ObsLard{}, err
	}

	return ObsLard{
		ID:                obs.ID,
		ObsTime:           obs.ObsTime,
		Data:              &val,
		KVFlagUseInfo:     DIURNAL_INTERPOLATED_USEINFO,
		KVFlagControlInfo: "00000" + "01000000005",
	}, nil
}

func (self *Obs) flagsAreInvalid() bool {
	if len(self.Flags) != 5 {
		return false
	}
	return !IsReal(self.Flags)
}

func IsReal(n string) bool {
	_, err := strconv.ParseFloat(n, 64)
	return err == nil
}
