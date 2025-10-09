package port

import (
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"time"

	csv "github.com/gocarina/gocsv"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
)

var TEXT_RECORD_FIELDS int = reflect.ValueOf(TextRecord{}).NumField()
var FLOAT_RECORD_FIELDS int = reflect.ValueOf(FloatRecord{}).NumField()

type TextRecord struct {
	Obstime  time.Time
	Original string
	Tbtime   string // unused
}

func NewTextRecord(csv []string) (*TextRecord, error) {
	if len(csv) != TEXT_RECORD_FIELDS {
		return nil, fmt.Errorf("Expected %d columns, got %d", TEXT_RECORD_FIELDS, len(csv))
	}

	obstime, err := time.Parse(time.RFC3339, csv[0])
	if err != nil {
		return nil, err
	}

	return &TextRecord{
		Obstime:  obstime,
		Original: csv[1],
		Tbtime:   csv[2],
	}, nil
}

type FloatRecord struct {
	Obstime     time.Time
	Original    float64
	Tbtime      string // unused
	Corrected   float64
	Controlinfo string
	Useinfo     string
	Cfailed     string
}

func NewFloatRecord(csv []string) (*FloatRecord, error) {
	if len(csv) != FLOAT_RECORD_FIELDS {
		return nil, fmt.Errorf("Expected %d columns, got %d", FLOAT_RECORD_FIELDS, len(csv))
	}

	_obstime := csv[0]
	_original := csv[1]
	_tbtime := csv[2]
	_corrected := csv[3]
	_controlinfo := csv[4]
	_useinfo := csv[5]
	_cfailed := csv[6]

	obstime, err := time.Parse(time.RFC3339, _obstime)
	if err != nil {
		return nil, err
	}

	original, err := strconv.ParseFloat(_original, 64)
	if err != nil {
		return nil, err
	}

	corrected, err := strconv.ParseFloat(_corrected, 64)
	if err != nil {
		return nil, err
	}

	return &FloatRecord{
		Obstime:     obstime,
		Original:    original,
		Tbtime:      _tbtime,
		Corrected:   corrected,
		Controlinfo: _controlinfo,
		Useinfo:     _useinfo,
		Cfailed:     _cfailed,
	}, nil
}

// NOTE:
// - for both kvalobs and histkvalobs:
//   - all stinfo non-scalar params that can be found in Kvalobs are stored in `text_data`
//   - 305, 306, 307, 308 are also in `data` but should be treated as `text_data` -> Special Cloud Types
//
// - only for histkvalobs
//   - 2751, 2752, 2753, 2754 are in `text_data` but should be treated as `data`? -> Metar Cloud types
//
// TODO: I'm not sure these params should be scalars given that the other cloud types are not.
// Should all cloud types be integers or text?

// Function for paramids 2751, 2752, 2753, 2754 that were stored as text data
// but should instead be treated as scalars
func parseMetarCloudType(tsid int64, nRecords int, reader csv.CSVReader) (*lard.ParsedCsv, error) {
	parsed := lard.NewParsedCsv(nRecords)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		record, err := NewTextRecord(fields)
		if err != nil {
			return nil, err
		}

		original, err := strconv.ParseFloat(record.Original, 64)
		if err != nil {
			return nil, err
		}

		// TODO: Original text obs were not flagged, so we don't return flags?
		// Or should we return default values?
		out := &lard.LegacyObs{
			Id:       tsid,
			Obstime:  record.Obstime,
			Original: &original,
		}

		parsed.Append(out.ToRow())
	}

	return parsed, nil
}

// Function for paramids 305, 306, 307, 308 that were stored as scalar data
// but should be treated as text
func parseSpecialCloudType(tsid int64, nRecords int, reader csv.CSVReader) (*lard.ParsedCsv, error) {
	parsed := lard.NewParsedCsv(nRecords)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		record, err := NewFloatRecord(fields)
		if err != nil {
			return nil, err
		}

		text := fmt.Sprint(record.Original)

		// TODO: should also return the flags somehow?
		out := &lard.TextObs{
			Id:      tsid,
			Obstime: record.Obstime,
			Text:    &text,
		}
		parsed.Append(out.ToRow())
	}

	return parsed, nil
}

func parseText(tsid int64, nRecords int, reader csv.CSVReader) (*lard.ParsedCsv, error) {
	parsed := lard.NewParsedCsv(nRecords)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		record, err := NewTextRecord(fields)
		if err != nil {
			return nil, err
		}

		out := lard.TextObs{
			Id:      tsid,
			Obstime: record.Obstime,
			Text:    &record.Original,
		}

		parsed.Append(out.ToRow())
	}

	return parsed, nil
}

func parseData(tsid int64, nRecords int, reader csv.CSVReader) (*lard.ParsedCsv, error) {
	parsed := lard.NewParsedCsv(nRecords)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		record, err := NewFloatRecord(fields)
		if err != nil {
			return nil, err
		}

		var originalPtr, correctedPtr *float64

		// Filter out special values that in Kvalobs stand for null observations
		if !slices.Contains(kvalobs.NULL_VALUES, record.Original) {
			originalPtr = &record.Original
		}
		if !slices.Contains(kvalobs.NULL_VALUES, record.Corrected) {
			correctedPtr = &record.Corrected
		}

		var cfailed *string
		if record.Cfailed != "" {
			cfailed = &record.Cfailed
		}

		qualityCode := lard.GetQualityCode(record.Useinfo)

		out := lard.LegacyObs{
			Id:          tsid,
			Obstime:     record.Obstime,
			Original:    originalPtr,
			Corrected:   correctedPtr,
			QualityCode: qualityCode,
			Controlinfo: &record.Controlinfo, // Never null, has default value in Kvalobs
			Useinfo:     &record.Useinfo,     // Never null, has default value in Kvalobs
			Cfailed:     cfailed,
		}

		parsed.Append(out.ToRow())
	}

	return parsed, nil
}
