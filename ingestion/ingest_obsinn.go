package main

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"gitlab.met.no/oda/oda/internal/pkg/oda"
	"gitlab.met.no/oda/oda/internal/pkg/odalog"
	stinfosys "gitlab.met.no/oda/stinfo-facade/pkg/stinfosys"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	kldataHeaderRex = regexp.MustCompile(`nationalnr=([0-9]+)/type=([0-9]+)`)
	kldataCSVRex    = regexp.MustCompile(`([^(),]+)(\([0-9]+,[0-9]+\))?`)
)

// Note: we only actually get one ObsChunk out of this, but I'm returning an
// array of them to keep a uniform signature with processBufrMessage
func processKldataMessage(msg *sarama.ConsumerMessage) ([]ObsChunk, error) {
	_, meta, body, err := getFormatMetaAndBody(msg.Value)
	if err != nil {
		odalog.Erroln(err)
		odalog.Erroln("for message:", msg.Value)
		return nil, errors.New("Failed to get format/meta/body")
	}
	obs, err := kldataParser(meta, body)
	if err != nil {
		odalog.Erroln(err)
		return nil, errors.New("Failed to parse kldata")
	}

	return []ObsChunk{{
		Offset:       msg.Offset,
		Observations: obs,
	}}, nil
}

func kldataParser(meta, body []byte) ([]Obs, error) {

	// parse metadata
	// example: kldata/nationalnr=53280/type=510/add
	match := kldataHeaderRex.FindSubmatch(meta)
	if len(match) != 3 {
		return nil, fmt.Errorf("%s:%w", string(body), ErrInvalidKafkaMsg)
	}
	stationID, err := strconv.ParseInt(string(match[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", string(body), ErrInvalidKafkaMsg)
	}
	typeID, err := strconv.ParseInt(string(match[2]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s:%w", string(body), ErrInvalidKafkaMsg)
	}

	// csv header, body
	// example: BR,DD,ERF,ERI,ERW,FF,FG_010,FX_1,QLI_01,QLO_01,QNET_01,QSI_01,QSO_01,RI,RR_010,RTS_1,SA,TA,TAN_010,TAX_010,TD,TF,TJ,TV,UU,VMOR(0,0),VMOR(1,0)
	//          20191231142000,,0,,,,0.0,,,,,,,,0.0,0.00,,70.0,-1.6,,,-3.1,,0.0,-2.1,89.7,2000.0,
	splt := bytes.SplitN(body, []byte("\n"), 2)
	if len(splt) != 2 {
		return nil, fmt.Errorf("%s:%w", string(body), ErrInvalidKafkaMsg)
	}
	csvHead, csvBody := splt[0], splt[1]

	//odalog.Println("New kldata\n" + string(meta) + "\n" + string(csvHead) + "\n" + string(csvBody))

	columns := kldataCSVRex.FindAll(csvHead, -1) // columns is an array of paramcodes.
	rows := bytes.Split(csvBody, []byte("\n"))   // rows are lines with #columns of parameter values.
	observations := make([]Obs, 0, len(rows)*len(columns)+2)

	KLOBSindex := -1
	MLATindex := -1
	MLONindex := -1

	for i := 0; i < len(columns); i++ {
		if matched, err := regexp.MatchString(`^KLOBS([(][0-9]+[,][0-9]+[)])?$`, string(columns[i])); KLOBSindex < 0 && err == nil && matched {
			KLOBSindex = i
			continue
		}
		if matched, err := regexp.MatchString(`^MLAT([(][0-9]+[,][0-9]+[)])?$`, string(columns[i])); MLATindex < 0 && err == nil && matched {
			MLATindex = i
			continue
		}
		if matched, err := regexp.MatchString(`^MLON([(][0-9]+[,][0-9]+[)])?$`, string(columns[i])); MLONindex < 0 && err == nil && matched {
			MLONindex = i
			continue
		}
	}

	if MLATindex == -1 {
		MLONindex = -1
	} else {
		if MLONindex == -1 {
			MLATindex = -1
		}
	}

	// loop over rows (observation times)
	for _, row := range rows {

		values := bytes.Split(row, []byte(","))
		if len(values) <= 1 {
			continue
		}

		// obstime
		obstime, err := time.Parse("20060102150405", string(values[0]))
		if err != nil {
			odalog.Warnln("trouble with:\n", "row:", string(row))
			odalog.Erroln("unable to parse obstime:", string(values[0]))
			continue
		}

		var realobstime *time.Time

		if KLOBSindex >= 0 {
			realobstimex, err := time.Parse(string("20060102150405000000")[:len(string(values[KLOBSindex+1]))], string(values[KLOBSindex+1]))
			if err == nil && len(string(values[KLOBSindex+1])) >= 8 {
				realobstime = &realobstimex
			} else {
				KLOBSindex = -1
			}
		}
		var mlat float32
		var mlon float32
		if MLATindex >= 0 {
			mlatx, err := (strconv.ParseFloat(string(values[MLATindex+1]), 32))
			if err == nil {
				mlat = float32(mlatx)
			} else {
				MLATindex = -1
				MLONindex = -1
			}
		}
		if MLONindex > 0 {
			mlonx, err := strconv.ParseFloat(string(values[MLONindex+1]), 32)
			if err == nil {
				mlon = float32(mlonx)
			} else {
				MLATindex = -1
				MLONindex = -1
			}
		}
		// loop over columns
		for i, value := range values[1:] {

			obs := Obs{
				ObsTime: obstime,
				ID: ObsID{
					StationIDType: "nationalnummer",
					StationID:     stationID,
					TypeID:        typeID,
				},
			}

			if KLOBSindex >= 0 && realobstime != nil {
				obs.OrgObstime = timestamppb.New(*realobstime)
			}
			if MLATindex >= 0 {
				obs.Lat = &mlat
				obs.Lon = &mlon
			}

			strValue := string(bytes.Trim(value, " "))
			if strValue == "" {
				continue
			}

			var value float64

			// sensor, level
			var sensor, level int64
			var sensorLevel string
			match := kldataCSVRex.FindSubmatch(columns[i])
			if len(match) < 2 {
				odalog.Erroln("bad parameter code:", columns[i])
				continue
			}
			code := string(bytes.Trim(match[1], " \n\t"))
			if len(match) == 3 && len(match[2]) > 0 {
				sensorLevel = string(match[2])
				strSplt := strings.Split(strings.Trim(sensorLevel, "()"), ",")
				sensor, err = strconv.ParseInt(strSplt[0], 10, 64)
				if err != nil {
					odalog.Erroln("invalid sensor value:", strSplt[0])
					continue
				}
				level, err = strconv.ParseInt(strSplt[1], 10, 64)
				if err != nil {
					odalog.Erroln("invalid sensor value:", strSplt[1])
					continue
				}
			}
			obs.ID.Sensor, obs.ID.Level = sensor, level

			// paramid from code
			paramID, _ := stinfosys.ParameterIDFromParameterCode(code) // defaults to 0
			obs.ID.ParameterID = paramID
			obs.ID.ParameterCode = code

			// parse strValue
			if paramID == 0 {
				// odalog.Warnln(code, "is an unknown parameter, parsing", strValue, "as text")
				obs.TextValue = []byte(strValue)
			} else if stinfosys.ParameterIsScalar(paramID) {
				trimmed := strings.Trim(strValue, " ")
				value, err = strconv.ParseFloat(trimmed, 64)
				if err != nil {
					odalog.Erroln("while parsing scalar value", strValue, "for", code, ":", err)
				}
				obs.FloatValue = oda.NewFloat64(value)
				obs.IsScalar = true
				obs.OriginalFormat = "kldata"
			} else {
				obs.TextValue = []byte(strValue)
			}
			observations = append(observations, obs)
		}
	}

	return observations, nil
}
