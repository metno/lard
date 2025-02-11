package port

import (
	"bufio"
	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

func parseDataCSV(tsid int64, file *os.File) ([][]any, [][]any, error) {
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	data := make([][]any, 0, rowCount)
	flags := make([][]any, 0, rowCount)
	var originalPtr, correctedPtr *float64
	for scanner.Scan() {
		// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
		// We don't parse tbtime
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, nil, err
		}

		original, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, nil, err
		}

		corrected, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, nil, err
		}

		// Filter out special values that in Kvalobs stand for null observations
		if !slices.Contains(kvalobs.NULL_VALUES, original) {
			originalPtr = &original
		}
		if !slices.Contains(kvalobs.NULL_VALUES, corrected) {
			correctedPtr = &corrected
		}

		// Original value is inserted in main data table
		lardObs := lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    originalPtr,
		}

		var cfailed *string
		if fields[6] != "" {
			cfailed = &fields[6]
		}

		flag := lard.Flag{
			Id:          tsid,
			Obstime:     obstime,
			Original:    originalPtr,
			Corrected:   correctedPtr,
			Controlinfo: &fields[4], // Never null, has default value in Kvalobs
			Useinfo:     &fields[5], // Never null, has default value in Kvalobs
			Cfailed:     cfailed,
		}

		data = append(data, lardObs.ToRow())
		flags = append(flags, flag.ToRow())
	}

	return data, flags, nil
}

// Text obs are not flagged
func parseTextCSV(tsid int64, file *os.File) ([][]any, error) {
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, err
		}

		lardObs := lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		}

		data = append(data, lardObs.ToRow())
	}

	return data, nil
}

// Function for paramids 2751, 2752, 2753, 2754 that were stored as text data
// but should instead be treated as scalars
// TODO: I'm not sure these params should be scalars given that the other cloud types are not.
// Should all cloud types be integers or text?
func parseMetarCloudType(tsid int64, file *os.File) ([][]any, error) {
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, err
		}

		original, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, err
		}

		lardObs := lard.DataObs{
			Id:      tsid,
			Obstime: obstime,
			Data:    &original,
		}

		data = append(data, lardObs.ToRow())
	}

	// TODO: Original text obs were not flagged, so we don't return a flags?
	// Or should we return default values?
	return data, nil
}

// Function for paramids 305, 306, 307, 308 that were stored as scalar data
// but should be treated as text
func parseSpecialCloudType(tsid int64, file *os.File) ([][]any, error) {
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	data := make([][]any, 0, rowCount)
	for scanner.Scan() {
		// obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
		// TODO: should parse everything and return the flags?
		fields := strings.Split(scanner.Text(), ",")

		obstime, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return nil, err
		}

		lardObs := lard.TextObs{
			Id:      tsid,
			Obstime: obstime,
			Text:    &fields[1],
		}

		data = append(data, lardObs.ToRow())
	}

	return data, nil
}
