package main

import (
	"errors"
	"strconv"

	"github.com/rickb777/period"
)

func makeDataPage(kdvh KDVHData) (Observation, error) {
	var useinfo, controlinfo []byte
	var nullData, blobData bool

	floatval, err := strconv.ParseFloat(kdvh.data, 64)
	if err != nil {
		if kdvh.data == "" {
			nullData = true
		} else {
			blobData = true
		}
	}

	// set flags
	if AreFlagsInvalid(kdvh.flags) {
		useinfo = []byte("9999900900000000")
	} else {
		useinfo = []byte(kdvh.flags + "00900000000")
	}
	if !nullData {
		controlinfo = []byte("0000000000000000")
	} else {
		controlinfo = []byte("0000003000000000")
		floatval = -32767
	}

	// TODO: figure out this stuff
	if blobData {
		return Observation{
			ID:                kdvh.ID,
			ObsTime:           kdvh.obsTime,
			DataBlob:          []byte(kdvh.data),
			KVFlagUseInfo:     useinfo,
			KVFlagControlInfo: controlinfo,
			KDVHFlag:          []byte(kdvh.flags),
		}, nil
	}

	return Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              floatval,
		CorrKDVH:          floatval,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
		KDVHFlag:          []byte(kdvh.flags),
	}, nil
}

// modify obstimes to always use totime
func makeDataPageProduct(kdvh KDVHData) (Observation, error) {
	obs, err := makeDataPage(kdvh)
	if !kdvh.offset.IsZero() {
		if temp, ok := kdvh.offset.AddTo(obs.ObsTime); ok {
			obs.ObsTime = temp
		}
	}
	return obs, err
}

// write flags correctly for T_EDATA
func makeDataPageEdata(kdvh KDVHData) (obs Observation, err error) {
	var useinfo, controlinfo []byte
	var floatval float64

	nullData := (kdvh.data == "")
	if !nullData {
		floatval, err = strconv.ParseFloat(kdvh.data, 64)
		if err != nil {
			nullData = true
		}
	}

	switch kdvh.flags {
	case "70000":
		useinfo = []byte("7000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			floatval = -32767
		}
	case "70101":
		useinfo = []byte("7010100900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			floatval = -32767
		}
	case "70381":
		useinfo = []byte("7038100900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000002000000000") //
			floatval = -32766                        //
		}
	case "70389":
		useinfo = []byte("7038900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000002000000000") //
			floatval = -32766                        //
		}
	case "90989":
		useinfo = []byte("9098900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000002000000000") //
			floatval = -32766                        //
		}
	case "99999":
		useinfo = []byte("9999900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			floatval = -32767
		}
	default:
		if AreFlagsInvalid(kdvh.flags) {
			useinfo = []byte("9999900900000000")
		} else {
			useinfo = []byte(kdvh.flags + "00900000000")
		}
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			floatval = -32767
		}
	}

	obs = Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              floatval,
		CorrKDVH:          floatval,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
		KDVHFlag:          []byte(kdvh.flags),
	}
	return obs, nil
}

func makeDataPagePdata(kdvh KDVHData) (obs Observation, err error) {
	var useinfo, controlinfo []byte
	var original, corrected float64

	nullData := (kdvh.data == "")
	if !nullData {
		floatval, err := strconv.ParseFloat(kdvh.data, 64)
		if err != nil {
			nullData = true
		}
		original = floatval
		corrected = original
	}

	switch kdvh.flags {
	case "00000":
		useinfo = []byte("0000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "10000":
		useinfo = []byte("1000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000001") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "30000":
		useinfo = []byte("3000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000001") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40000":
		useinfo = []byte("4000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000001") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "50000":
		useinfo = []byte("5000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "60000":
		useinfo = []byte("6000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70000":
		useinfo = []byte("7000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70103":
		useinfo = []byte("7010300900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70203":
		useinfo = []byte("7020300900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "50205":
		useinfo = []byte("7020500900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "10319":
		useinfo = []byte("1031900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "30319":
		useinfo = []byte("3031900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40319":
		useinfo = []byte("4031900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "99319":
		useinfo = []byte("9931900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "10329":
		useinfo = []byte("1032900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70381":
		useinfo = []byte("7038100900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "71381":
		useinfo = []byte("7038100900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "50383":
		useinfo = []byte("5038300900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "20389":
		useinfo = []byte("2038900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "30389":
		useinfo = []byte("3038900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "40389":
		useinfo = []byte("4038900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000") //
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "90909":
		useinfo = []byte("9090900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40929":
		useinfo = []byte("4092900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "48929":
		useinfo = []byte("4892900900000000")
		if !nullData {
			controlinfo = []byte("0000001000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "48999":
		useinfo = []byte("4899900900000000")
		if !nullData {
			controlinfo = []byte("0000001000000005") //
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "71000":
		useinfo = []byte("7100000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "71203":
		useinfo = []byte("7120300900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "99999":
		useinfo = []byte("9999900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	default:
		if AreFlagsInvalid(kdvh.flags) {
			useinfo = []byte("9999900900000000")
		} else {
			useinfo = []byte(kdvh.flags + "00000000000")
		}
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	}

	obs = Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              original,
		CorrKDVH:          corrected,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
		KDVHFlag:          []byte(kdvh.flags),
	}
	return obs, nil
}

func makeDataPageNdata(kdvh KDVHData) (obs Observation, err error) {
	var useinfo, controlinfo []byte
	var original, corrected float64

	nullData := (kdvh.data == "")
	if !nullData {
		floatval, err := strconv.ParseFloat(kdvh.data, 64)
		if err != nil {
			nullData = true
		}
		original = floatval
		corrected = original
	}

	switch kdvh.flags {
	case "30319":
		useinfo = []byte("3031900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "38929":
		useinfo = []byte("3892900900000000")
		if !nullData {
			controlinfo = []byte("0000001000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "40000":
		useinfo = []byte("4000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000001")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40100":
		useinfo = []byte("4010000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000001")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40315":
		useinfo = []byte("4031500900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "40319":
		useinfo = []byte("4031900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "43325":
		useinfo = []byte("4332500900000000")
		if !nullData {
			controlinfo = []byte("0000004000000006")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "48325":
		useinfo = []byte("4832500900000000")
		if !nullData {
			controlinfo = []byte("0000001000000006")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "49225":
		useinfo = []byte("4922500900000000")
		if !nullData {
			controlinfo = []byte("0000000000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "49915":
		useinfo = []byte("4991500900000000")
		if !nullData {
			controlinfo = []byte("0000000000000005")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70000":
		useinfo = []byte("7000000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70204":
		useinfo = []byte("7020400900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "70389":
		useinfo = []byte("7038900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000")
		} else {
			controlinfo = []byte("0000002000000000") //
			original = -32766                        //
			corrected = original
		}
	case "71000":
		useinfo = []byte("7100000900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "73309":
		useinfo = []byte("7330900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "78937":
		useinfo = []byte("7893700900000000")
		if !nullData {
			controlinfo = []byte("0000001000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "90909":
		useinfo = []byte("9090900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "93399":
		useinfo = []byte("9339900900000000")
		if !nullData {
			controlinfo = []byte("0000004000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	case "98999":
		useinfo = []byte("9899900900000000")
		if !nullData {
			controlinfo = []byte("0000001000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
		}
		corrected = -32767 //
	case "99999":
		useinfo = []byte("9999900900000000")
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	default:
		if AreFlagsInvalid(kdvh.flags) {
			useinfo = []byte("9999900900000000")
		} else {
			useinfo = []byte(kdvh.flags + "00000000000")
		}
		if !nullData {
			controlinfo = []byte("0000000000000000")
		} else {
			controlinfo = []byte("0000003000000000")
			original = -32767
			corrected = original
		}
	}

	obs = Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              original,
		CorrKDVH:          corrected,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
		KDVHFlag:          []byte(kdvh.flags),
	}
	return obs, nil
}

func makeDataPageVdata(kdvh KDVHData) (obs Observation, err error) {
	var useinfo, controlinfo []byte
	var floatval float64

	// set useinfo based on time
	if h := kdvh.obsTime.Hour(); h == 0 || h == 6 || h == 12 || h == 18 {
		useinfo = []byte("4000000900000000")
	} else {
		useinfo = []byte("9999900900000000")
	}

	// set data and controlinfo
	nullData := (kdvh.data == "")
	if !nullData {
		floatval, err = strconv.ParseFloat(kdvh.data, 64)
		if err != nil {
			nullData = true
		}
	}
	if !nullData {
		controlinfo = []byte("0000000000000000")
	} else {
		controlinfo = []byte("0000003000000000")
		floatval = -32767
	}

	// super special treatment clause of T_VDATA.OT_24, so it will be the same as in kvalobs
	if kdvh.elemCode == "OT_24" {
		// add custom offset, because OT_24 in KDVH has been treated differently than OT_24 in kvalobs
		offset, err := period.Parse("PT18H") // fromtime_offset -PT6H, timespan P1D
		if err != nil {
			return Observation{}, errors.New("could not parse period")
		}
		temp, ok := offset.AddTo(kdvh.obsTime)
		if !ok {
			return Observation{}, errors.New("could not add period")
		}

		kdvh.obsTime = temp
		// convert from hours to minutes...
		floatval = floatval * 60
	}

	obs = Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              floatval,
		CorrKDVH:          floatval,
		KVFlagUseInfo:     useinfo,
		KVFlagControlInfo: controlinfo,
		KDVHFlag:          []byte(kdvh.flags),
	}
	return obs, nil
}

func makeDataPageDiurnalInterpolated(kdvh KDVHData) (obs Observation, err error) {
	corrected, err := strconv.ParseFloat(kdvh.data, 64)
	if err != nil {
		return Observation{}, err
	}
	obs = Observation{
		ID:                kdvh.ID,
		ObsTime:           kdvh.obsTime,
		Data:              -32767,
		CorrKDVH:          corrected,
		KVFlagUseInfo:     []byte("4892500900000000"),
		KVFlagControlInfo: []byte("0000001000000005"),
	}
	return obs, nil
}

func AreFlagsInvalid(flags string) bool {
	return len(flags) != 5 || !IsReal([]byte(flags))
}

func IsReal(n []byte) bool {
	if len(n) > 0 && n[0] == '-' {
		n = n[1:]
	}
	if len(n) == 0 {
		return false
	}
	var point bool
	for _, c := range n {
		if '0' <= c && c <= '9' {
			continue
		}
		if c == '.' && len(n) > 1 && !point {
			point = true
			continue
		}
		return false
	}
	return true
}
