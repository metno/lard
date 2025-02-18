package port

import (
	"errors"
	"strconv"

	"github.com/rickb777/period"

	kdvh "migrate/kdvh/db"
	"migrate/kdvh/import/flags"
	"migrate/lard"
)

// Workaround to return reference to consts
func addr[T any](t T) *T {
	return &t
}

func flagsAreValid(obs *kdvh.Obs) bool {
	if len(obs.Flags) != 5 {
		return false
	}
	_, err := strconv.ParseInt(obs.Flags, 10, 64)
	return err == nil
}

func useinfo(obs *kdvh.Obs) *string {
	if !flagsAreValid(obs) {
		return addr(flags.INVALID)
	}
	return addr(obs.Flags + flags.DELAY_DEFAULT)
}

// Default ConvertFunction
// NOTE: this should be the only function that can return `lard.TextObs` with non-null text data.
func convert(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	var valPtr *float64

	controlinfo := flags.VALUE_PASSED_QC
	if obs.Data == "" {
		controlinfo = flags.VALUE_MISSING
	}

	val, err := strconv.ParseFloat(obs.Data, 64)
	if err == nil {
		valPtr = &val
	}

	return &ParsedObs{
		&lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		&lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
		},
		&lard.LegacyData{
			Corrected: valPtr,
		},
		&lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}}, nil
}

// This function modifies obstimes to always use totime
// This is needed because KDVH used incorrect and incosistent timestamps
func convertProduct(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	parsed, err := convert(obs, ts)
	if !ts.Offset.IsZero() {
		if temp, ok := ts.Offset.AddTo(parsed.data.Obstime); ok {
			parsed.data.Obstime = temp
			parsed.text.Obstime = temp
			parsed.flag.Obstime = temp
		}
	}
	return parsed, err
}

func convertEdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	var controlinfo string
	var valPtr *float64

	if val, err := strconv.ParseFloat(obs.Data, 64); err != nil {
		switch obs.Flags {
		case "70381", "70389", "90989":
			controlinfo = flags.VALUE_REMOVED_BY_QC
		default:
			// Includes "70000", "70101", "99999"
			controlinfo = flags.VALUE_MISSING
		}
	} else {
		controlinfo = flags.VALUE_PASSED_QC
		valPtr = &val
	}

	return &ParsedObs{
		&lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		&lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
		},
		&lard.LegacyData{
			Corrected: valPtr,
		},
		&lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}}, nil
}

func convertPdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	var controlinfo string
	var valPtr *float64

	if val, err := strconv.ParseFloat(obs.Data, 64); err != nil {
		switch obs.Flags {
		case "20389", "30389", "40389", "50383", "70381", "71381":
			controlinfo = flags.VALUE_REMOVED_BY_QC
		default:
			// "00000", "10000", "10319", "30000", "30319",
			// "40000", "40929", "48929", "48999", "50000",
			// "50205", "60000", "70000", "70103", "70203",
			// "71000", "71203", "90909", "99999"
			controlinfo = flags.VALUE_MISSING
		}
	} else {
		valPtr = &val

		switch obs.Flags {
		case "10319", "10329", "30319", "40319", "48929", "48999":
			controlinfo = flags.VALUE_MANUALLY_INTERPOLATED
		case "20389", "30389", "40389", "50383", "70381", "71381", "99319":
			controlinfo = flags.VALUE_CORRECTED_AUTOMATICALLY
		case "40929":
			controlinfo = flags.INTERPOLATION_ADDED_MANUALLY
		default:
			// "71000", "71203", "90909", "99999"
			controlinfo = flags.VALUE_PASSED_QC
		}
	}
	return &ParsedObs{
		&lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		&lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
		},
		&lard.LegacyData{
			Corrected: valPtr,
		},
		&lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}}, nil
}

func convertNdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	var controlinfo string
	var valPtr *float64

	if val, err := strconv.ParseFloat(obs.Data, 64); err != nil {
		switch obs.Flags {
		case "70389":
			controlinfo = flags.VALUE_REMOVED_BY_QC
		default:
			// "30319", "38929", "40000", "40100", "40315"
			// "40319", "43325", "48325", "49225", "49915"
			// "70000", "70204", "71000", "73309", "78937"
			// "90909", "93399", "98999", "99999"
			controlinfo = flags.VALUE_MISSING
		}
	} else {
		valPtr = &val

		switch obs.Flags {
		case "43325", "48325":
			controlinfo = flags.VALUE_MANUALLY_ASSIGNED
		case "30319", "38929", "40315", "40319":
			controlinfo = flags.VALUE_MANUALLY_INTERPOLATED
		case "49225", "49915":
			controlinfo = flags.INTERPOLATION_ADDED_MANUALLY
		case "70389", "73309", "78937", "93399", "98999":
			controlinfo = flags.VALUE_CORRECTED_AUTOMATICALLY
		default:
			// "40000", "40100", "70000", "70204", "71000", "90909", "99999"
			controlinfo = flags.VALUE_PASSED_QC
		}
	}

	return &ParsedObs{
		&lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		&lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
		},
		&lard.LegacyData{
			Corrected: valPtr,
		},
		&lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: &controlinfo,
			Useinfo:     useinfo(obs),
		}}, nil
}

func convertVdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	// set useinfo based on time
	if h := obs.Obstime.Hour(); h == 0 || h == 6 || h == 12 || h == 18 {
		useinfo = flags.COMPLETED_HQC
	} else {
		useinfo = flags.INVALID
	}

	// set data and controlinfo
	if val, err := strconv.ParseFloat(obs.Data, 64); err != nil {
		controlinfo = flags.VALUE_MISSING
	} else {
		// super special treatment clause of T_VDATA.OT_24, so it will be the same as in kvalobs
		// add custom offset, because OT_24 in KDVH has been treated differently than OT_24 in kvalobs
		if ts.Element == "OT_24" {
			offset, err := period.Parse("PT18H") // fromtime_offset -PT6H, timespan P1D
			if err != nil {
				return nil, errors.New("could not parse period")
			}
			temp, ok := offset.AddTo(obs.Obstime)
			if !ok {
				return nil, errors.New("could not add period")
			}

			obs.Obstime = temp
			// convert from hours to minutes
			val *= 60.0
		}

		valPtr = &val
		controlinfo = flags.VALUE_PASSED_QC
	}

	return &ParsedObs{
		&lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		&lard.TextObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Text:    &obs.Data,
		},
		&lard.LegacyData{
			Corrected: valPtr,
		},
		&lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

func convertDiurnalInterpolated(obs *kdvh.Obs, ts *kdvh.TsInfo) (*ParsedObs, error) {
	val, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		return nil, err
	}
	return &ParsedObs{
		data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    &val,
		},
		legacy: &lard.LegacyData{
			Corrected: &val,
		},
		flag: &lard.LegacyFlag{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Controlinfo: addr(flags.VALUE_MANUALLY_INTERPOLATED),
			Useinfo:     addr(flags.DIURNAL_INTERPOLATED_USEINFO),
		}}, nil
}
