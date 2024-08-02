package main

import (
	// TODO: maybe use sqlx?
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

func (self *DumpArgs) elementQuery() string {
	var out string
	if self.Elements == nil {
		return out
	}

	for _, val := range self.Elements {
		out += fmt.Sprintf(",%v", val)
	}
	return out
}

func (self *DumpArgs) Execute(args []string) error {
	// defer EmailOnPanic("dumpData")
	self.cmdListsToSlices()

	// TODO: need different connections depending on table.FromKlima11 (or define url directly in TableInstructions)
	conn, err := sql.Open("oracle", os.Getenv("KDVH_STRING"))
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD_HH24:MI:SS'")
	if err != nil {
		return err
	}

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for name, table := range TABLE2INSTRUCTIONS {
		if self.Tables != nil && !slices.Contains(self.Tables, name) {
			continue
		}

		// TODO: are these the same for data and flags table?
		if self.Stations == nil {
			// TODO: this should modify self.Stations?
			fetchStationNumbers(table.TableName, conn)
		}

		// dump data table
		if !self.SkipData {
			dumpTable(table.TableName, table.SplitQuery, conn, self)
		}

		// dump flag table
		if !self.SkipFlags && table.FlagTableName != "" {
			dumpTable(table.FlagTableName, table.SplitQuery, conn, self)
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
			log.Println("Could not parse station number")
			continue
		}

		if fetchByYear {
			fetchDataByYear(tableName, stnr, conn, args)
		} else {
			fetchData(tableName, stnr, conn, args)
		}

		// original method
		//   - write 1 file per column for each station (did it make it easier to import later?)
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

func fetchStationNumbers(tableName string, conn *sql.DB) {
	rows, err := conn.Query("SELECT DISTINCT stnr FROM :1 ORDER BY stnr", tableName)
	if err != nil {
		// TODO: handle error
		return
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
}

func fetchData(tableName string, station int64, conn *sql.DB, args *DumpArgs) {
	// TODO: potential injection :)
	query := fmt.Sprintf("SELECT stnr,dato,%s FROM :1 WHERE stnr = :2 ORDER BY dato", args.elementQuery())

	if tableName == "T_HOMOGEN_MONTH" {
		// hack for T_HOMOGEN_MONTH, to single out the month values
		query = "SELECT stnr,dato,tam,rr FROM :1 WHERE stnr = :2 AND SEASON BETWEEN 1 AND 12 ORDER BY dato"
	}

	rows, err := conn.Query(query, tableName, station)
	if err != nil {
		// TODO: handle error
		return
	}
	defer rows.Close()

	file, err := os.Create(fmt.Sprintf("%v%v/%v.csv", args.BaseDir, tableName, station))
	if err != nil {
		fmt.Println("Could not create file:", err)
		return
	}

	err = writeRows(rows, file)
	if err != nil {
		log.Println("Error writing csv", err)
	}
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

func fetchDataByYear(tableName string, station int64, conn *sql.DB, args *DumpArgs) {
	// TODO: potential injection :)
	query := fmt.Sprintf("SELECT stnr,dato,%s FROM :1 WHERE stnr = :2 AND TO_CHAR(dato, 'yyyy') = :3 ORDER BY dato", args.elementQuery())

	begin, end, err := fetchYearRange(tableName, station, conn)
	if err != nil {
		// TODO: handle error
		return
	}

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, tableName, station, string(year))
		if err != nil {
			// TODO: handle error
			return
		}

		// TODO: scan rows
		rows.Close()
	}
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
		timeFormat := "2006-01-02 15:04:05"
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
