package flags

// In kvalobs a flag is a 16 char string containg QC information about the observation:
// Note: Missing numbers in the following lists are marked as reserved (not in use I guess?)
//
// CONTROLINFO FLAG:
//
//  0 - CONTROL LEVEL (not used)
//  1 - RANGE CHECK
//   0. Not checked
//   1. Check passed
//   2. Higher than HIGH
//   3. Lower than LOW
//   4. Higher than HIGHER
//   5. Lower that LOWER
//   6. Check failed, above HIGHEST or below LOWEST
//
//  2 - FORMAL CONSISTENCY CHECK
//    0. Not checked
//    1. Check passe
//    2. Inconsistency found, but not an error with the relevant parameter, no correction
//    3. Inconsistency found at the observation time, but not possible to determine which parameter, no correction
//    4. Inconsistency found at earliar/later observation times, but not possible to determine which parameter, no correction
//    6. Inconsistency found at the observation time, probably error with the relevant parameter, no correction
//    7. Inconsistency found at earliar/later observation times, probably error with relevant parameter, no correction
//    8. Inconsistency found, a parameter is missing, no correction
//    A. Inconsistency found at the observation time, corrected automatically
//    B. Inconsistency found at earliar/later observation times, corrected automatically
//    D. Check failed
//
//  3 - JUMP CHECK (STEP, DIP, FREEZE, DRIFT)
//    0. Not checked
//    1. Check passed
//    2. Change higher than test value, no correction
//    3. No change in measured value (freeze check did not pass?), no correction
//    4. Suspected error in freeze check, no error in dip check (??), no correction
//    5. Suspected error in dip check, no error in freeze check (??), no correction
//    7. Observed drift, no correction
//    9. Change higher than test value, corrected automatically
//    A. Freeze check did not pass, corrected automatically
//
//  4 - PROGNOSTIC CHECK
//    0. Not checked
//    1. Check passed
//    2. Deviation from model higher than HIGH
//    3. Deviation from model lower than LOW
//    4. Deviation from model higher than HIGHER
//    5. Deviation from model lower that LOWER
//    6. Check failed, deviation from model above HIGHEST or below LOWEST
//
//  5 - VALUE CHECK (FOR MOVING STATIONS)
//    0. Not checked
//    1. Check passed
//    3. Suspicious value, no correction
//    4. Suspicious value, corrected automatically
//    6. Check failed
//
//  6 - MISSING OBSERVATIONS
//    0. Original and corrected values exist
//    1. Original value missing, but corrected value exists
//    2. Corrected value missing, orginal value discarded
//    3. Original and corrected values missing
//
//  7 - TIMESERIES FITTING
//    0. Not checked
//    1. Interpolated with good fitness
//    2. Interpolated with unsure fitness
//    3. Intepolation not suitable
//
//  8 - WEATHER ANALYSIS
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//    3. Suspicious value, corrected automatically
//
//  9 - STATISTICAL CHECK
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//
// 10 - CLIMATOLOGICAL CONSISTENCY CHECK
//    0. Not checked
//    1. Check passed
//    2. Climatologically questionable, but not an error with the relevant parameter, no correction
//    3. Climatologically questionable at the observation time, but not possible to determine which parameter, no correction
//    4. Climatologically questionable at earliar/later observation times, but not possible to determine which parameter, no correction
//    6. Climatologically questionable at the observation time, probably error with the relevant parameter, no correction
//    7. Climatologically questionable at earliar/later observation times, probably error with relevant parameter, no correction
//    A. Inconsistency found at the observation time, corrected automatically
//    B. Inconsistency found at earliar/later observation times, corrected automatically
//    D. Check failed
//
// 11 - CLIMATOLOGICAL CHECK
//    0. Not checked
//    1. Check passed
//    2. Suspicious value, not corrected
//    3. Suspicious value, corrected automatically
//
// 12 - DISTRIBUTION CHECK OF ACCUMULATED PARAMETERS (ESPECIALLY FOR PRECIPITATION)
//    0. Not checked
//    1. Not an accumulated value
//    2. Observation outside accumulated parameter range
//    3. Abnormal observation (??)
//    6. Accumulation calculated from numerical model
//    7. Accumulation calculated from weather analysis
//    A. Accumulation calculated with 'steady rainfall' method
//    B. Accumulation calculated with 'uneven rainfall' method
//
// 13 - PREQUALIFICATION (CERTAIN PAIRS OF 'STATIONID' AND 'PARAMID' CAN BE DISCARDED)
//    0. Not checked
//    5. Value is missing
//    6. Check failed, invalid original value
//    7. Check failed, original value is noisy
//
// 14 - COMBINATION CHECK
//    0. Not checked
//    1. Check passed
//    2. Outside test limit value, but no jumps detected and inside numerical model tolerance
//    9. Check failed. Outside test limit value, no jumps detected but outside numerical model tolerance
//    A. Check failed. Outside test limit value, jumps detected but inside numerical model tolerance
//    B. Check failed. Outside test limit value, jumps detected and outside numerical model tolerance
//
// 15 - MANUAL QUALITY CONTROL
//    0. Not checked
//    1. Check passed
//    2. Probably OK
//    5. Value manually interpolated
//    6. Value manually assigned
//    7. Value manually corrected
//    A. Manually rejected

const (
	VALUE_PASSED_QC = "00000" + "00000000000"

	// Corrected value is present, and original was remove by QC
	VALUE_CORRECTED_AUTOMATICALLY = "00000" + "01000000000"
	VALUE_MANUALLY_INTERPOLATED   = "00000" + "01000000005"
	VALUE_MANUALLY_ASSIGNED       = "00000" + "01000000006"

	VALUE_REMOVED_BY_QC = "00000" + "02000000000" // Corrected value is missing, and original was remove by QC
	VALUE_MISSING       = "00000" + "03000000000" // Both original and corrected are missing
	VALUE_PASSED_HQC    = "00000" + "00000000001" // Value was sent to HQC for inspection, but it was OK

	// original value still exists, not exactly sure what the difference with VALUE_MANUALLY_INTERPOLATED is
	INTERPOLATION_ADDED_MANUALLY = "00000" + "00000000005"
)

// USEINFO FLAG:
//
//  0 - CONTROL LEVELS PASSED
//    1. Completed QC1, QC2 and HQC
//    2. Completed QC2 and HQC
//    3. Completed QC1 and HQC
//    4. Completed HQC
//    5. Completed QC1 and QC2
//    6. Completed QC2
//    7. Completed QC1
//    9. Missing information
//
//  1 - DEVIATION FROM NORM (MEAN?)
//    0. Observation time and period are okay
//    1. Observation time deviates from norm
//    2. Observation period is shorter than norm
//    3. Observation perios is longer than norm
//    4. Observation time deviates from norm, and period is shorter than norm
//    5. Observation time deviates from norm, and period is longer than norm
//    8. Missing value
//    9. Missing status information
//
//  2 - QUALITY LEVEL OF ORIGNAL VALUE
//    0. Value is okay
//    1. Value is suspicious (probably correct)
//    2. Value is suspicious (probably wrong)
//    3. Value is wrong
//    9. Missing quality information
//
//  3 - TREATMENT OF ORIGINAL VALUE
//    0. Unchanged
//    1. Manually corrected
//    2. Manually interpolated
//    3. Automatically corrected
//    4. Automatically interpolated
//    5. Manually derived from accumulated value
//    6. Automatically derived from accumulated value
//    8. Rejected
//    9. Missing information
//
//  4 - MOST IMPORT CHECK RESULT (?)
//    0. Original value is okay
//    1. Range check
//    2. Consistency check
//    3. Jump check
//    4. Consistency check in relation with earlier/later observations
//    5. Prognostic check based on observation data
//    6. Prognostic check based on Timeseries
//    7. Prognostic check based on model data
//    8. Prognostic check based on statistics
//    9. Missing information
//
//  7 - DELAY INFORMATION
//    0. Observation carried out and reported at the right time
//    1. Observation carried out early and reported at the right time
//    2. Observation carried out late and reported at the right time
//    3. Observation reported early
//    4. Observation reported late
//    5. Observation carried out early and reported late
//    6. Observation carried out late and reported late
//    9. Missing information
//
//  8 - FIRST DIGIT OF HEXADECIMAL VALUE OF THE OBSERVATION CONFIDENCE LEVEL
//  9 - SECOND DIGIT OF HEXADECIMAL VALUE OF THE OBSERVATION CONFIDENCE LEVEL
// 13 - FIRST HQC OPERATOR DIGIT
// 14 - SECOND HQC OPERATOR DIGIT
// 15 - HEXADECIMAL DIGIT WITH NUMBER OF TESTS THAT DID NOT PASS (RETURNED A RESULT?)

const (
	// Remaing 11 digits of `useinfo` that follow the 5 digits contained in `obs.Flags`.
	// TODO: From the docs it looks like the '9' should be changed by kvalobs when
	// the observation is inserted into the database but that's not the case?
	DELAY_DEFAULT = "00900000000"

	INVALID                      = "99999" + DELAY_DEFAULT // Only returned when the flags are invalid
	COMPLETED_HQC                = "40000" + DELAY_DEFAULT // Specific to T_VDATA
	DIURNAL_INTERPOLATED_USEINFO = "48925" + DELAY_DEFAULT // Specific to T_DIURNAL_INTERPOLATED
)
