package port

import (
	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"slices"
	"strconv"
	"strings"
	"time"
)

// NOTE:
// - for both kvalobs and histkvalobs:
//      - all stinfo non-scalar params that can be found in Kvalobs are stored in `text_data`
//      - 305, 306, 307, 308 are also in `data` but should be treated as `text_data` -> Special Cloud Types
// - only for histkvalobs
//      - 2751, 2752, 2753, 2754 are in `text_data` but should be treated as `data`? -> Metar Cloud types
// TODO: I'm not sure these params should be scalars given that the other cloud types are not.
// Should all cloud types be integers or text?

// Function for paramids 2751, 2752, 2753, 2754 that were stored as text data
// but should instead be treated as scalars
func parseMetarCloudType(tsid int64, row string) (*lard.ParsedObs, error) {
	// obstime, original, tbtime
	fields := strings.Split(row, ",")

	obstime, err := time.Parse(time.RFC3339, fields[0])
	if err != nil {
		return nil, err
	}

	original, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}

	// TODO: Original text obs were not flagged, so we don't return a flags?
	// Or should we return default values?
	return &lard.ParsedObs{
		Data: &lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    &original,
		},
	}, nil

}

// Function for paramids 305, 306, 307, 308 that were stored as scalar data
// but should be treated as text
func parseSpecialCloudType(tsid int64, row string) (*lard.ParsedObs, error) {
	// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
	// TODO: should parse everything and return the flags?
	fields := strings.Split(row, ",")

	obstime, err := time.Parse(time.RFC3339, fields[0])
	if err != nil {
		return nil, err
	}

	return &lard.ParsedObs{
		Text: &lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		},
	}, nil
}

func parseText(tsid int64, row string) (*lard.ParsedObs, error) {
	fields := strings.Split(row, ",")

	obstime, err := time.Parse(time.RFC3339, fields[0])
	if err != nil {
		return nil, err
	}

	return &lard.ParsedObs{
		Text: &lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		},
	}, nil
}

func parseData(tsid int64, row string) (*lard.ParsedObs, error) {
	var originalPtr, correctedPtr *float64

	// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
	// We don't parse tbtime
	fields := strings.Split(row, ",")

	obstime, err := time.Parse(time.RFC3339, fields[0])
	if err != nil {
		return nil, err
	}

	original, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}

	corrected, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return nil, err
	}

	// Filter out special values that in Kvalobs stand for null observations
	if !slices.Contains(kvalobs.NULL_VALUES, original) {
		originalPtr = &original
	}
	if !slices.Contains(kvalobs.NULL_VALUES, corrected) {
		correctedPtr = &corrected
	}

	var cfailed *string
	if fields[6] != "" {
		cfailed = &fields[6]
	}

	useinfo := fields[5]
	qualityCode, _ := lard.GetQualityCode(useinfo)

	return &lard.ParsedObs{
		// Original value is inserted in main data table
		Data: &lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    originalPtr,
			// QcUsable: qcUsable
		},
		Legacy: &lard.LegacyData{
			Id:          tsid,
			Obstime:     obstime,
			Corrected:   correctedPtr,
			QualityCode: qualityCode,
		},
		Flag: &lard.LegacyFlag{
			Id:          tsid,
			Obstime:     obstime,
			Controlinfo: &fields[4], // Never null, has default value in Kvalobs
			Useinfo:     &useinfo,   // Never null, has default value in Kvalobs
			Cfailed:     cfailed,
		},
	}, nil
}
