package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	// "sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	_ "github.com/jackc/pgx/v5/stdlib"
	// go_ora "github.com/sijms/go-ora/v2"
)

type DumpArgs struct {
	Sep         string   `long:"sep" default:";"  description:"Separator character in the dumped files"`
	BaseDir     string   `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	SkipData    bool     `long:"skipdata" description:"If given, the values from dataTable will NOT be processed"`
	SkipFlags   bool     `long:"skipflag" description:"If given, the values from flagTable will NOT be processed"`
	Overwrite   bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Combine     bool     `long:"combine" description:"Combine data and flags timeseries in a single file for import"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
}

// Populates args slices by splitting cmd strings
func (args *DumpArgs) setupConfig() {
	if len(args.Sep) > 1 {
		log.Println("--sep= accepts only single characters. Defaulting to ';'")
		args.Sep = ";"
	}

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

func testJoins(conn *sql.DB, config *DumpArgs) {
	table := TABLE2INSTRUCTIONS["T_TJ_DATA"]

	// outdir := filepath.Join(config.BaseDir, table.TableName)
	// if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
	// 	log.Println("Skipping data dump of", table.TableName, "because dumped folder already exists")
	// 	return
	// }

	path := "./test_joins"
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		log.Println(err)
	}

	element := "tj1"
	station := "7420"

	query1 := fmt.Sprintf(`
        SELECT
		    COALESCE(d.dato, f.dato) AS time,
		    d.%[1]s AS data,
		    f.%[1]s AS flag
		FROM
		    (SELECT dato, %[1]s FROM %[2]s WHERE stnr = $1) d
		FULL OUTER JOIN
		    (SELECT dato, %[1]s FROM %[3]s WHERE stnr = $1) f
            ON d.dato = f.dato
            ORDER BY time`,
		element,
		table.TableName,
		table.FlagTableName,
	)

	filename := filepath.Join(path, "query1.csv")
	if err := testDump(filename, query1, station, table, conn, config); err != nil {
		log.Println(err)
		return
	}

	query2 := fmt.Sprintf(`
	       SELECT
		    COALESCE(d.dato, f.dato) AS time,
		    d.%[1]s AS data,
		    f.%[1]s AS flag
		FROM
		    (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
		FULL OUTER JOIN
		    (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
            ON d.dato = f.dato
            ORDER BY time`,
		element,
		table.TableName,
		table.FlagTableName,
	)

	filename = filepath.Join(path, "query2.csv")
	if err := testDump(filename, query2, station, table, conn, config); err != nil {
		log.Println(err)
		return
	}

}

func testDump(filename, query, station string, table *TableInstructions, conn *sql.DB, config *DumpArgs) error {
	rows, err := conn.Query(query, station)
	if err != nil {
		log.Println("Could not query KDVH:", err)
		return err
	}

	if err = writeElementFile(rows, filename, config.Sep); err != nil {
		log.Println(err)
	} else {
		log.Printf("%s - station %s dumped successfully", table.TableName, station)
	}
	return nil
}

func (args *DumpArgs) Execute(_ []string) error {
	args.setupConfig()

	// TODO: using the PG proxy would let us get rid of the double connection
	// dvhConn := getDB(os.Getenv("DVH_STRING"))
	// klima11Conn := getDB(os.Getenv("KLIMA11_STRING"))

	// TODO: abstract away driver, so we can connect both with pgx and go_ora (if need be)?
	conn, err := sql.Open("pgx", os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		log.Println(err)
		return nil
	}

	// testJoins(conn, args)
	// return nil

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	// var wg sync.WaitGroup
	for _, table := range TABLE2INSTRUCTIONS {
		// wg.Add(1)
		// go func(table *TableInstructions) {
		// 	defer wg.Done()
		if args.Tables != nil && !slices.Contains(args.Tables, table.TableName) {
			continue
		}

		log.Println("Starting dump of", table.TableName)
		// setLogFile(table.TableName, "dump")

		// Instead of doing everything that is below this comment, I'd rather simply use this
		// SELECT
		//      COALESCE(d.dato, f.dato) AS date,
		//      COALESCE(d.stnr, f.stnr) AS station,
		//      d.tj1 AS data,
		//      f.tj1 AS flag
		// FROM
		//      (SELECT dato, stnr, tj1 FROM t_tj_data WHERE tj1 IS NOT NULL) d
		// FULL OUTER JOIN
		//      (SELECT dato, stnr, tj1 FROM t_tj_flag WHERE tj1 IS NOT NULL) f
		//      ON d.dato = f.dato
		// ORDER BY station;
		// (this might be too big of a query)
		//
		//or
		//
		// SELECT DISTINCT stnr FROM t_tj_data WHERE tj1 IS NOT NULL;
		// SELECT
		//      COALESCE(d.dato, f.dato) AS date,
		//      d.tj1 AS data,
		//      f.tj1 AS flag
		// FROM
		//      (SELECT dato, stnr, tj1 FROM t_tj_data WHERE tj1 IS NOT NULL AND stnr = xxx) d
		// FULL OUTER JOIN
		//      (SELECT dato, stnr, tj1 FROM t_tj_flag WHERE tj1 IS NOT NULL AND stnr = xxx) f
		//      ON d.dato = f.dato;
		//
		// I'm not sure if it'll have worse performance though

		// elements, err := fetchColumnNames(table.TableName, conn)
		// if err != nil {
		// 	log.Printf("Could not fetch column names for table %s: %v", table.TableName, err)
		// 	continue
		// 	// return
		// }
		//
		// elements = filterSlice(args.Elements, elements)

		dumpTable(table, conn, args)

		// if !args.SkipFlags && table.FlagTableName != "" {
		// 	flagElements, err := fetchColumnNames(table.FlagTableName, conn)
		// 	if err != nil {
		// 		log.Printf("Could not fetch column names for table %s: %v", table.TableName, err)
		// 		continue
		// 		// return
		// 	}
		//
		// 	flagElements = filterSlice(args.Elements, flagElements)
		// 	dumpTable(table.FlagTableName, stations, flagElements, table.SplitQuery, conn, args)
		// }

		// not needed anymore
		// if args.Combine {
		// 	combineDataAndFlags(table, args)
		// }

		// }(table)

		// log.SetOutput(os.Stdout)
		log.Println("Finished dump of", table.TableName)
	}
	// wg.Wait()

	return nil
}

// Get connection pool with Oracle connection string
// func getDB(connString string) *sql.DB {
// 	connector := go_ora.NewConnector(connString)
// 	return sql.OpenDB(connector)
// }

// Creates the comma separated list of elements we want to SELECT
func querySelect(elements []string) string {
	out := "dato"
	for _, e := range elements {
		out += fmt.Sprintf(",%v", e)
	}
	return out
}

func dumpTable(table *TableInstructions, conn *sql.DB, config *DumpArgs) {
	defer sendEmailOnPanic("dumpTable", config.Email)

	outdir := filepath.Join(config.BaseDir, table.TableName)
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		log.Println("Skipping data dump of", table.TableName, "because dumped folder already exists")
		return
	}

	// TODO:
	elements, err := fetchColumnNames(table.TableName, conn)
	if err != nil {
		log.Printf("Could not fetch column names for table %s: %v", table.TableName, err)
		return
	}
	elements = filterSlice(config.Elements, elements)

	// Avoid nil
	flagElements := make([]string, 0)

	if table.FlagTableName != "" {
		flagElements, err = fetchColumnNames(table.TableName, conn)
		if err != nil {
			log.Printf("Could not fetch column names for table %s: %v", table.FlagTableName, err)
			return
		}
		flagElements = filterSlice(config.Elements, flagElements)
	}

	for _, element := range elements {
		stations, err := fetchStationNumbers(table.TableName, element, conn)
		if err != nil {
			log.Printf("Could not fetch stations for table %s: %v", table.TableName, err)
			continue
		}
		stations = filterSlice(config.Stations, stations)

		dumpDataOnly := table.FlagTableName == "" || !slices.Contains(flagElements, element)
		for _, station := range stations {
			var err error

			if table.SplitQuery {
				err = dumpStationByYear(element, station, table, dumpDataOnly, conn, config)
			} else {
				err = dumpStation(element, station, table, dumpDataOnly, conn, config)
			}

			if err != nil {
				log.Println("Could not dump data:", err)
			} else {
				log.Printf("%s - station %s dumped successfully", table.TableName, station)
			}
		}

	}
}

// TODO: what's difference between obs_origtime and klobs?
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season"}

// Fetch column names for a given table
func fetchColumnNames(tableName string, conn *sql.DB) ([]string, error) {
	log.Printf("Fetching elements for %s...", tableName)

	rows, err := conn.Query(
		// in PG
		"SELECT column_name FROM information_schema.columns WHERE table_name = $1 and NOT column_name = ANY($2::text[])",
		// in Oracle
		// "select column_name from all_tab_columns where table_name = :1",
		// NOTE: needs to be lowercase with PG
		strings.ToLower(tableName),
		INVALID_COLUMNS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var elements []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			log.Println("Could not scan column name:", err)
			continue
		}
		elements = append(elements, name)
	}
	return elements, nil
}

// FIXME: this can be extremely slow
func fetchStationNumbers(tableName, element string, conn *sql.DB) ([]string, error) {
	log.Println("Fetching station numbers (this can take a while)...")

	query := fmt.Sprintf("SELECT DISTINCT stnr FROM %s WHERE %s IS NOT NULL", tableName)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stations := make([]string, 0)
	for rows.Next() {
		var stnr string

		if err := rows.Scan(&stnr); err != nil {
			log.Println("Could not scan station number:", err)
			continue
		}

		stations = append(stations, stnr)
	}

	log.Println("Finished fetching station numbers!")
	return stations, nil
}

// Fetch min and max year from table
func fetchYearRange(tableName, station string, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string

	// in PG
	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = $1", tableName)
	// in Oracle
	// query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = :1", tableName)

	if err := conn.QueryRow(query, station).Scan(&beginStr, &endStr); err != nil {
		log.Println("Could not query row:", err)
		return 0, 0, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		log.Printf("Could not parse year '%s': %s", beginStr, err)
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		log.Printf("Could not parse year '%s': %s", endStr, err)
		return 0, 0, err
	}

	return begin, end, nil
}

// FIXME: broken
func dumpStationByYear(element, station string, table *TableInstructions, dumpDataOnly bool, conn *sql.DB, config *DumpArgs) error {
	// query := fmt.Sprintf(
	// in PG
	// "SELECT %s FROM %s WHERE stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2 ORDER BY dato",
	// in Oracle
	// "SELECT %s FROM %s WHERE stnr = :1 AND TO_CHAR(dato, 'yyyy') = :2 ORDER BY dato",
	// 	querySelect(elements),
	// 	tableName,
	// )

	var query string

	// TODO: need min, max both flag and table
	begin, end, err := fetchYearRange(table.TableName, station, conn)
	if err != nil {
		return err
	}

	if dumpDataOnly {
		query = fmt.Sprintf(
			"SELECT dato AS time, %[1]s AS data FROM %[2]s WHERE %[1] IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2",
			element,
			table.TableName,
		)
	} else {
		query = fmt.Sprintf(
			`SELECT
                COALESCE(d.dato, f.dato) AS time,
                d.%[1]s AS data,
                f.%[1]s AS flag
            FROM
                (SELECT dato, stnr, %[1]s FROM %[2]s 
                    WHERE 
                        %[1]s IS NOT NULL 
                    AND 
                        stnr = $1 
                    AND 
                        TO_CHAR(dato, 'yyyy') = $2) d
            FULL OUTER JOIN
                (SELECT dato, stnr, %[1]s FROM %[3]s 
                    WHERE 
                        %[1]s IS NOT NULL 
                    AND 
                        stnr = $1 
                    AND 
                        TO_CHAR(dato, 'yyyy') = $2) f
                ON d.dato = f.dato`,
			element,
			table.TableName,
			table.FlagTableName,
		)

		flagBegin, flagEnd, err := fetchYearRange(table.TableName, station, conn)
		if err != nil {
			return err
		}

		begin = min(begin, flagBegin)
		end = max(end, flagEnd)
	}

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, station, string(year))
		if err != nil {
			log.Println("Could not query KDVH:", err)
			return err
		}

		path := filepath.Join(config.BaseDir, tableName, string(station), string(year))
		if err = writeElementFiles(rows, path, config.Sep); err != nil {
			log.Println(err)
			return err
		}
	}

	log.Printf("%s - station %s dumped successfully", tableName, station)
	return nil
}

func dumpStation(element, station string, table *TableInstructions, dumpDataOnly bool, conn *sql.DB, config *DumpArgs) error {
	var query string

	// Hack for T_HOMOGEN_MONTH to single out months
	if table.TableName == "T_HOMOGEN_MONTH" {
		// TODO: why do we only need tam and rr?
		// query_old := "SELECT dato,tam,rr FROM T_HOMOGEN_MONTH WHERE stnr = $1 AND season BETWEEN 1 AND 12"
		query = fmt.Sprintf(
			"SELECT dato AS time, %s[1]s AS data FROM T_HOMOGEN_MONTH WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12",
			element,
		)
	} else if dumpDataOnly {
		query = fmt.Sprintf(
			"SELECT dato AS time, %[1]s AS data FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1",
			element,
			table.TableName,
		)
	} else {
		query = fmt.Sprintf(
			`SELECT
                COALESCE(d.dato, f.dato) AS time,
                d.%[1]s AS data,
                f.%[1]s AS flag
            FROM
                (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
            FULL OUTER JOIN
                (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
                ON d.dato = f.dato`,
			// The following query keeps also the cases where both data and flag are NULL
			// I don't see the benefit in using it, but it depends on what we want to do at import time
			// query := fmt.Sprintf(`
			//        SELECT
			// 	    COALESCE(d.dato, f.dato) AS time,
			// 	    d.%[1]s AS data,
			// 	    f.%[1]s AS flag
			// 	FROM
			// 	    (SELECT dato, %[1]s FROM %[2]s WHERE stnr = $1) d
			// 	FULL OUTER JOIN
			// 	    (SELECT dato, %[1]s FROM %[3]s WHERE stnr = $1) f
			//            ON d.dato = f.dato`,
			element,
			table.TableName,
			table.FlagTableName,
		)
	}

	rows, err := conn.Query(query, station)
	if err != nil {
		log.Println("Could not query KDVH:", err)
		return err
	}

	p := filepath.Join(config.BaseDir, table.TableName, string(station))
	if err = writeElementFiles(rows, p, config.Sep); err != nil {
		log.Println(err)
	} else {
		log.Printf("%s - station %s dumped successfully", table.TableName, station)
	}

	return nil
}

func writeElementFile(rows *sql.Rows, filename, sep string) error {
	columns, err := rows.Columns()
	if err != nil {
		return errors.New("Could not get columns: " + err.Error())
	}

	count := len(columns)

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// write header
	file.WriteString("time" + sep + "data" + sep + "flag" + "\n")

	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return errors.New("Could not scan rows: " + err.Error())
		}

		// Parse scanned types
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
			case nil:
				value = ""
			default:
				value = fmt.Sprintf("%v", v)
			}

			values[i] = value
		}

		// Write to file
		var line string
		if count == 2 {
			line = fmt.Sprintf("%s%s%s%s\n", values[0], sep, values[1], sep)
		} else {
			line = fmt.Sprintf("%s%s%s%s%s\n", values[0], sep, values[1], sep, values[2])
		}

		if _, err := file.WriteString(line); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	return rows.Err()

}

// Writes each element column (+ timestamp, which is column 0) in the queried table to separate files
func writeElementFiles(rows *sql.Rows, path, sep string) error {
	columns, err := rows.Columns()
	if err != nil {
		return errors.New("Could not get columns: " + err.Error())
	}

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}

	count := len(columns)
	files := make([]*os.File, count-1)

	for i := range files {
		filename := filepath.Join(path, columns[i+1]+".csv")
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		// write header
		file.WriteString(columns[0] + sep + columns[i+1] + "\n")
		files[i] = file
	}

	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return errors.New("Could not scan rows: " + err.Error())
		}

		// Parse scanned types
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
			case nil:
				value = ""
			default:
				value = fmt.Sprintf("%v", v)
			}

			values[i] = value
		}

		// Write to files
		for i, file := range files {
			if values[i+1] == "" {
				continue
			}

			line := fmt.Sprintf("%s%s%s\n", values[0], sep, values[i+1])

			if _, err := file.WriteString(line); err != nil {
				return errors.New("Could not write to file: " + err.Error())
			}
		}
	}

	return rows.Err()
}

// TODO: this shouldn't need stations/elements
func combineDataAndFlags(table *TableInstructions, config *DumpArgs) {
	outdir := filepath.Join(config.BaseDir, table.TableName+"_combined")
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		log.Println("Skipping combine step of", table.TableName, "because folder already exists")
		return
	}

	log.Println("Combining data and flags...")

	path := filepath.Join(config.BaseDir, table.TableName)
	stations, err := os.ReadDir(path)
	if err != nil {
		log.Printf("Could not read directory %s: %s", path, err)
		return
	}

	// loop over station dirs
	for _, station := range stations {
		stationdir := filepath.Join(outdir, station.Name())
		if err := os.MkdirAll(stationdir, os.ModePerm); err != nil {
			log.Println(err)
			return
		}

		elements, err := os.ReadDir(stationdir)
		if err != nil {
			log.Printf("Could not read directory %s: %s", stationdir, err)
			return
		}

		for _, element := range elements {
			dataFile := filepath.Join(config.BaseDir, table.TableName, station.Name(), element.Name())
			// gota.
			// data := dataframe.ReadCSV(dataFile)
			data, err := readFile(dataFile, config.Sep)
			if err != nil {
				log.Println(err)
				continue
			}

			if data.Nrow() == 0 {
				log.Printf("%s - %s - %s - No data found, skipping!", table.TableName, station, element)
				continue
			}

			var flags dataframe.DataFrame
			if table.FlagTableName != "" {
				flagFile := filepath.Join(config.BaseDir, table.FlagTableName, station.Name(), element.Name())
				flags, err = readFile(flagFile, config.Sep)
				// It's okay to skip flag if not present
				// if err != nil {
				// 	log.Println(err)
				// 	continue
				// }

				// TODO: this is not ideal
				// if len(data) != len(flags) {
				// 	log.Printf("Different number of rows (%v vs %v)\n", len(data), len(flags))
				// 	continue
				// }
			}

			outfile := filepath.Join(stationdir, element.Name())

			// TODO: replace with gota
			if err := writeCombined(data, flags, outfile, config.Sep); err != nil {
				log.Printf("ERROR: %s - %s - %s - %s", table.TableName, station.Name(), element.Name(), err)
				continue
			}

			log.Printf("%s - %s - %s - combined data was written to %s", table.TableName, station.Name(), element.Name(), outfile)
		}
	}
}

// write a new file using data (and optionally flag) files where each line is formatted as "timestamp<sep>data<sep>(flag)"
// func writeCombined(data, flags [][]string, filename, sep string) error {
func writeCombined(data, flags dataframe.DataFrame, filename, sep string) error {
	outfile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer outfile.Close()

	var outdf dataframe.DataFrame
	if data.Nrow() > flags.Nrow() {
		outdf = data.LeftJoin(flags, "X0")
	} else {
		outdf = data.RightJoin(flags, "X0")
	}

	return outdf.WriteCSV(outfile, dataframe.WriteHeader(false))

	// for i := range data {
	// 	line := fmt.Sprintf("%s%s%s%s", data[i][0], sep, data[i][1], sep)
	// 	if flags != nil {
	// 		if i < len(flags) {
	// 			if flags[i][0] != data[i][0] {
	// 				return errors.New("ERROR: Different timestamps in data and flag files")
	// 			}
	// 			line += flags[i][1]
	// 		}
	// 	}
	//
	// 	if _, err := outfile.WriteString(line + "\n"); err != nil {
	// 		return err
	// 	}
	// }
	// return nil
}
