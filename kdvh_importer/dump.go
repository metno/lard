package main

import (
	// TODO: maybe use sqlx?
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

type DumpArgs struct {
	BaseDir     string `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep         string `long:"sep" default:";"  description:"Separator character in the dumped files"`
	TablesCmd   string `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	SkipData    bool   `long:"skipdata" description:"If given, the values from dataTable will NOT be processed"`
	SkipFlags   bool   `long:"skipflag" description:"If given, the values from flagTable will NOT be processed"`
	Limit       int32  `long:"limit" default:"0" description:"If given, the procedure will stop after migrating this number of stations"`
	Overwrite   bool   `long:"overwrite" description:"Overwrite any existing dumped files"`
	Combine     bool
	Tables      []string
	Stations    []string
	Elements    []string
}

func (args *DumpArgs) cmdListsToSlices() {
	if args.TablesCmd != "" {
		args.Tables = strings.Split(args.TablesCmd, ",")
	}
	if args.StationsCmd != "" {
		args.Stations = strings.Split(args.StationsCmd, ",")
	}
	if args.ElementsCmd != "" {
		args.Elements = strings.Split(args.ElementsCmd, ",")
	}
}

func (args *DumpArgs) elementQuery() string {
	var out string
	if args.Elements == nil {
		return out
	}

	// TODO: should val be converted to lowercase?
	for _, e := range args.Elements {
		out += fmt.Sprintf(",%v", e)
	}
	return out
}

func (args *DumpArgs) Execute(_ []string) error {
	// defer EmailOnPanic("dumpData")
	args.cmdListsToSlices()

	// TODO: need different connections depending on table.FromKlima11? (or define url directly in TableInstructions?)
	conn, err := sql.Open("oracle", os.Getenv("KDVH_STRING"))
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: this should last for the whole connection
	_, err = conn.Exec("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD_HH24:MI:SS'")
	if err != nil {
		return err
	}

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for name, table := range TABLE2INSTRUCTIONS {
		if args.Tables != nil && !slices.Contains(args.Tables, name) {
			continue
		}
		// TODO: are these the same for data and flags table?
		if args.StationsCmd == "" {
			err = args.fetchStationNumbers(name, conn)
			if err != nil {
				log.Printf("Could not fetch stations for table %s: %v", name, err)
				continue
			}
		}
		if !args.SkipData {
			dumpTable(table.TableName, table.SplitQuery, conn, args)
		}
		if !args.SkipFlags && table.FlagTableName != "" {
			dumpTable(table.FlagTableName, table.SplitQuery, conn, args)
		}
		if args.Combine && !(args.SkipData && args.SkipFlags) {
			err := combineTables(table, args)
			if err != nil {
				// TODO:
			}
		}
	}
	return nil
}

func dumpTable(tableName string, fetchByYear bool, conn *sql.DB, args *DumpArgs) {
	combDir := args.BaseDir + tableName
	if _, err := os.ReadDir(combDir); err == nil && !args.Overwrite {
		log.Println("Skipping data dump of", tableName, "because dumped folder already exists")
		return
	}

	log.Println("Starting data dump of", tableName)
	setLogFile(tableName, "dump")

	for _, station := range args.Stations {
		stnr, err := strconv.ParseInt(station, 10, 64)
		if err != nil {
			log.Println("Could not parse station number:", err)
			continue
		}

		if fetchByYear {
			err = dumpDataByYear(tableName, stnr, conn, args)
			if err != nil {
				log.Println("Could not dump data:", err)
				continue
			}
		} else {
			err = dumpData(tableName, stnr, conn, args)
			if err != nil {
				log.Println("Could not dump data:", err)
				continue
			}
		}

		// original method
		//   - write 1 file per column for each station (did it make it easier to import later? Same logic as PODA)
		//   - sort and combine column files for data and flags
		//   - import by looping over combined element files
		// TODO:
		// alternative method?
		//   - dump whole data and flag tables (sorted by 'dato') for each station
		//   - read in both dumps
		//   - import by looping over columns
		// Possible problems:
		//   - sort by 'dato' might lead to inconsistencies between data and flags table?
		//   - how to deal with blobs (do we have blobs?)
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished data dump of", tableName)
}

// TODO: probably useless
func fetchColumnNames(tableName string, conn *sql.DB, args *DumpArgs) {
	rows, err := conn.Query(
		// TODO: not sure this works for oracle
		"SELECT column_id, column_name FROM all_tab_columns WHERE table_name = :1 AND (:2 IS NULL OR elem_code = ANY(:2)) ",
		tableName, args.Elements,
	)
	if err != nil {
		return
	}

	defer rows.Close()

	//TODO: scan rows
}

func (args *DumpArgs) fetchStationNumbers(tableName string, conn *sql.DB) error {
	rows, err := conn.Query("SELECT DISTINCT stnr FROM :1 ORDER BY stnr", tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	// TODO: alternatively use sqlx.Select() to directly scan to slice
	stations := make([]string, 0)
	for rows.Next() {
		var stnr int32

		err := rows.Scan(&stnr)
		if err != nil {
			log.Println("Could not scan station row:", err)
			continue
		}

		stations = append(stations, fmt.Sprint(stnr))
	}

	args.Stations = stations
	return nil
}

func dumpData(tableName string, station int64, conn *sql.DB, args *DumpArgs) error {
	// TODO: potential injection :)
	query := fmt.Sprintf("SELECT stnr,dato%s FROM :1 WHERE stnr = :2 ORDER BY dato", args.elementQuery())

	// hack for T_HOMOGEN_MONTH, to single out the month values
	if tableName == "T_HOMOGEN_MONTH" {
		query = "SELECT stnr,dato,tam,rr FROM :1 WHERE stnr = :2 AND SEASON BETWEEN 1 AND 12 ORDER BY dato"
	}

	rows, err := conn.Query(query, tableName, station)
	if err != nil {
		// TODO: handle error
		return err
	}
	defer rows.Close()

	file, err := os.Create(fmt.Sprintf("%v%v/%v.csv", args.BaseDir, tableName, station))
	if err != nil {
		return fmt.Errorf("Could not create file: %v", err)
	}

	return writeRows(rows, file)
}

// Fetch min and max year from table
func fetchYearRange(tableName string, station int64, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string

	err := conn.QueryRow(
		"SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM :1 WHERE stnr = :2",
		tableName,
		station,
	).Scan(&beginStr, &endStr)

	if err != nil {
		// TODO: handle error
		return 0, 0, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		// TODO: handle error
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		// TODO: handle error
		return 0, 0, err
	}

	return begin, end, nil
}

func dumpDataByYear(tableName string, station int64, conn *sql.DB, args *DumpArgs) error {
	// TODO: potential injection :)
	query := fmt.Sprintf("SELECT stnr,dato%s FROM :1 WHERE stnr = :2 AND TO_CHAR(dato, 'yyyy') = :3 ORDER BY dato", args.elementQuery())

	begin, end, err := fetchYearRange(tableName, station, conn)
	if err != nil {
		// TODO: handle error
		return err
	}

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, tableName, station, string(year))

		if err != nil {
			// TODO: handle error
			return err
		}

		file, err := os.Create(fmt.Sprintf("%v%v/%v/%v.csv", args.BaseDir, tableName, station, year))
		if err != nil {
			return fmt.Errorf("Could not create file: %v", err)
		}

		err = writeRows(rows, file)
		if err != nil {
			return fmt.Errorf("Could not create file: %v", err)
		}
		rows.Close()
	}
	return nil
}

// This dumps the whole table
func writeRows(rows *sql.Rows, writer io.Writer) error {
	csvWriter := csv.NewWriter(writer)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	err = csvWriter.Write(columns)
	if err != nil {
		return fmt.Errorf("Could not write headers: %w", err)
	}

	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i := range columns {
			pointers[i] = &values[i]
		}

		if err = rows.Scan(pointers...); err != nil {
			return err
		}

		floatFormat := "%.2f"
		timeFormat := "2006-01-02_15:04:05"
		for i := range columns {
			var value string

			switch v := values[i].(type) {
			case []byte:
				value = string(v)
			case float64, float32:
				value = fmt.Sprintf(floatFormat, v)
			case time.Time:
				value = v.Format(timeFormat)
			default:
				value = fmt.Sprintf("%v", v)
			}

			row[i] = value
		}

		err = csvWriter.Write(row)
		if err != nil {
			return fmt.Errorf("Could not write row to csv: %w", err)
		}
	}

	err = rows.Err()
	csvWriter.Flush()
	return err
}

// TODO: is it better to do multiple queries for each element joining data and tables?
// Transpose table, so each row is a single timeseries
func transposeRows(rows *sql.Rows) ([]string, map[string][]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)
	elements := make(map[string][]string, count)

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err = rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}

		floatFormat := "%.2f"
		timeFormat := "2006-01-02_15:04:05"
		for i, col := range columns {
			var value string

			switch v := values[i].(type) {
			case []byte:
				value = string(v)
			case float64, float32:
				value = fmt.Sprintf(floatFormat, v)
			case time.Time:
				value = v.Format(timeFormat)
			default:
				value = fmt.Sprintf("%v", v)
			}

			elements[col] = append(elements[col], value)
		}
	}

	dates, ok := elements["dato"]
	if !ok {
		// TODO:
	}
	delete(elements, "dato")

	return dates, elements, rows.Err()
}

func writeAllTimeseriesSeparate(rows *sql.Rows, path string) error {
	obstime, timeseries, err := transposeRows(rows)
	if err != nil {
		return err
	}

	for element, ts := range timeseries {
		err := writeSingleTs(obstime, ts, fmt.Sprintf("%s/%s.csv", path, element))
		if err != nil {
			// TODO:
			continue
		}
	}
	return nil
}

func writeSingleTs(obstime []string, timeseries []string, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("Could not create file: %w", err)
	}
	defer file.Close()

	for i, obs := range timeseries {
		_, err := file.WriteString(fmt.Sprintf("%s,%s\n", obstime[i], obs))
		if err != nil {
			return fmt.Errorf("Could not write row to csv: %w", err)
		}
	}
	return nil
}

func combineTables(ti *TableInstructions, args *DumpArgs) error {
	outdir := fmt.Sprintf("%s/%s_combined", args.BaseDir, ti.TableName)
	// TODO: check file mode
	err := os.MkdirAll(outdir, os.ModePerm)
	if err != nil {
		// TODO:
	}
	for _, station := range args.Stations {
		for _, element := range args.Elements {
			data, err := readFile(fmt.Sprintf("%s/%s/%s/%s.csv", args.BaseDir, ti.TableName, station, element))
			if err != nil {
				// TODO:
			}
			flags, err := readFile(fmt.Sprintf("%s/%s/%s/%s.csv", args.BaseDir, ti.FlagTableName, station, element))
			if err != nil {
				// TODO:
			}
			if len(data) != len(flags) {
				// TODO:
			}
			err = writeCombined(data, flags, fmt.Sprintf("%s/%s/%s/%s.csv", args.BaseDir, outdir, station, element))
			if err != nil {
				// TODO:
			}
		}
	}
	return nil
}

func readFile(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return csv.NewReader(file).ReadAll()
}

func writeCombined(data [][]string, flags [][]string, filename string) error {
	outfile, err := os.Open("path_to_combined/station/element.csv")
	if err != nil {
		// TODO:
	}
	defer outfile.Close()

	for i := range data {
		_, err := outfile.WriteString(fmt.Sprintf("%s,%s,%s", data[i][0], data[i][1], flags[i][1]))
		if err != nil {
			// TODO:
		}
	}
	return nil
}
