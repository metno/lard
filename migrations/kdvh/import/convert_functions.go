package port

import (
	"errors"
	"strconv"

	"github.com/rickb777/period"

	kdvh "migrate/kdvh/db"
	"migrate/kdvh/import/flags"
	"migrate/lard"
)

func flagsAreValid(obs *kdvh.Obs) bool {
	if len(obs.Flags) != 5 {
		return false
	}
	_, err := strconv.ParseInt(obs.Flags, 10, 64)
	return err == nil
}

func extractUseinfo(obs *kdvh.Obs) string {
	if !flagsAreValid(obs) {
		return flags.INVALID
	}
	return obs.Flags + flags.DELAY_DEFAULT
}

// Default ConvertFunction
// NOTE: this should be the only function that can return `lard.TextObs` with non-null text data.
func convert(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
	var valPtr *float64

	controlinfo := flags.VALUE_PASSED_QC
	if obs.Data == "" {
		controlinfo = flags.VALUE_MISSING
	}

	useinfo := extractUseinfo(obs)
	qcCode := lard.GetQualityCode(useinfo)

	val, err := strconv.ParseFloat(obs.Data, 64)
	if err == nil {
		valPtr = &val
	}

	if !ts.IsScalar {
		return &lard.ParsedObs{
			Text: &lard.TextObs{
				Id:      ts.Id,
				Obstime: obs.Obstime,
				Text:    &obs.Data,
			}}, nil
	}

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   valPtr,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

// This function modifies obstimes to always use totime
// This is needed because KDVH used incorrect and incosistent timestamps
func convertProduct(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
	parsed, err := convert(obs, ts)
	if !ts.Offset.IsZero() {
		if temp, ok := ts.Offset.AddTo(parsed.Data.Obstime); ok {
			parsed.Data.Obstime = temp
			parsed.Text.Obstime = temp
		}
	}
	return parsed, err
}

func convertEdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
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

	useinfo := extractUseinfo(obs)
	qcCode := lard.GetQualityCode(useinfo)

	if !ts.IsScalar {
		return &lard.ParsedObs{
			Text: &lard.TextObs{
				Id:      ts.Id,
				Obstime: obs.Obstime,
				Text:    &obs.Data,
			}}, nil
	}

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   valPtr,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

func convertPdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
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

	useinfo := extractUseinfo(obs)
	qcCode := lard.GetQualityCode(useinfo)

	if !ts.IsScalar {
		return &lard.ParsedObs{
			Text: &lard.TextObs{
				Id:      ts.Id,
				Obstime: obs.Obstime,
				Text:    &obs.Data,
			}}, nil
	}

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   valPtr,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

func convertNdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
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

	useinfo := extractUseinfo(obs)
	qcCode := lard.GetQualityCode(useinfo)

	if !ts.IsScalar {
		return &lard.ParsedObs{
			Text: &lard.TextObs{
				Id:      ts.Id,
				Obstime: obs.Obstime,
				Text:    &obs.Data,
			}}, nil
	}

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   valPtr,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

func convertVdata(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
	var useinfo, controlinfo string
	var valPtr *float64

	// set useinfo based on time
	if h := obs.Obstime.Hour(); h == 0 || h == 6 || h == 12 || h == 18 {
		useinfo = flags.COMPLETED_HQC
	} else {
		useinfo = flags.INVALID
	}

	qcCode := lard.GetQualityCode(useinfo)

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

	if !ts.IsScalar {
		return &lard.ParsedObs{
			Text: &lard.TextObs{
				Id:      ts.Id,
				Obstime: obs.Obstime,
				Text:    &obs.Data,
			}}, nil
	}

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    valPtr,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   valPtr,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}

func convertDiurnalInterpolated(obs *kdvh.Obs, ts *kdvh.TsInfo) (*lard.ParsedObs, error) {
	val, err := strconv.ParseFloat(obs.Data, 64)
	if err != nil {
		return nil, err
	}

	controlinfo := flags.VALUE_MANUALLY_INTERPOLATED
	useinfo := flags.DIURNAL_INTERPOLATED_USEINFO
	qcCode := lard.GetQualityCode(useinfo)

	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      ts.Id,
			Obstime: obs.Obstime,
			Data:    &val,
		},
		Legacy: &lard.LegacyData{
			Id:          ts.Id,
			Obstime:     obs.Obstime,
			Corrected:   &val,
			QualityCode: qcCode,
			Controlinfo: &controlinfo,
			Useinfo:     &useinfo,
		}}, nil
}
