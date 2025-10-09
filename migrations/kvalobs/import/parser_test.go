package port

import (
	"encoding/csv"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"migrate/lard"
)

func TestParseData(t *testing.T) {
	var tsid int64 = 1
	obstime := "2006-01-01T06:00:00Z"
	original := -32767
	tbtime := "2006-01-01T07:00:00Z"
	corrected := -8.3
	controlinfo := "0000000000000007"
	useinfo := "4031900000000020"
	cfailed := "hqc"

	rowString := fmt.Sprintf("%s,%v,%s,%v,%s,%s,%s", obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed)

	_obstime, err := time.Parse(time.RFC3339, obstime)
	if err != nil {
		t.Fatal()
	}

	qualitcode := lard.GetQualityCode(useinfo)

	obs := lard.LegacyObs{
		Id:          tsid,
		Obstime:     _obstime,
		Original:    nil,
		Corrected:   &corrected,
		QualityCode: qualitcode,
		Controlinfo: &controlinfo,
		Useinfo:     &useinfo,
		Cfailed:     &cfailed,
	}

	expected := obs.ToRow()

	reader := csv.NewReader(
		strings.NewReader(rowString),
	)

	parsed, err := parseData(tsid, 1, reader)
	if err != nil {
		t.Fatal()
	}

	if len(parsed.Data) != 1 {
		t.Fatal("Parsed number of records != 1")
	}

	record := parsed.Data[0]
	// TODO: not sure how reliable this is
	if !reflect.DeepEqual(expected, record) {
		t.Fail()
	}

}
