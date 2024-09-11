package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	// "sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	// go_ora "github.com/sijms/go-ora/v2"
)

type TableDumpFunction func(dumpFuncArgs, *sql.DB) error
type dumpFuncArgs struct {
	path      string
	element   Element
	station   string
	dataTable string
	flagTable string
}

type DumpConfig struct {
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

// Populates config slices by splitting cmd strings
func (config *DumpConfig) setup() {
	if config.TablesCmd != "" {
		config.Tables = strings.Split(config.TablesCmd, ",")
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		config.Elements = strings.Split(config.ElementsCmd, ",")
	}
}

// Get connection pool with Oracle connection string
// func getDB(connString string) *sql.DB {
// 	connector := go_ora.NewConnector(connString)
// 	return sql.OpenDB(connector)
// }

func (config *DumpConfig) Execute(_ []string) error {
	config.setup()

	// TODO: make sure we don't need direct KDVH connection
	// dvhConn := getDB(os.Getenv("DVH_STRING"))
	// klima11Conn := getDB(os.Getenv("KLIMA11_STRING"))

	conn, err := sql.Open("pgx", os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		log.Println(err)
		return nil
	}

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	// var wg sync.WaitGroup
	for _, table := range TABLE2INSTRUCTIONS {
		// wg.Add(1)
		// go func(table *TableInstructions) {
		// 	defer wg.Done()
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		table.updateDefaults()
		dumpTable(table, conn, config)
		// }(table)
	}
	// wg.Wait()

	return nil
}

func dumpTable(table *TableInstructions, conn *sql.DB, config *DumpConfig) {
	defer sendEmailOnPanic("dumpTable", config.Email)

	// TODO: should probably do it at the station/element level?
	outdir := filepath.Join(config.BaseDir, table.TableName+"_combined")
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		log.Println("Skipping data dump of", table.TableName, "because dumped folder already exists")
		return
	}

	log.Println("Starting dump of", table.TableName)
	setLogFile(table.TableName, "dump")

	elements, err := getElements(table, conn)
	if err != nil {
		return
	}
	log.Println("Elements:", elements)
	elements = filterElements(config.Elements, elements)

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for _, element := range elements {
		stations, err := fetchStationsWithElement(table, element, conn)
		if err != nil {
			log.Printf("Could not fetch stations for table %s: %v", table.TableName, err)
			return
		}
		msg := fmt.Sprintf("Element '%s'", element.name) + "not available for station '%s'"
		stations = filterSlice(config.Stations, stations, msg)

		for _, station := range stations {
			path := filepath.Join(outdir, string(station))
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				log.Println(err)
				continue
			}

			err := table.DumpFunc(
				dumpFuncArgs{
					path:      path,
					element:   element,
					station:   station,
					dataTable: table.TableName,
					flagTable: table.FlagTableName,
				},
				conn,
			)
			if err == nil {
				log.Printf("%s - %s - %s dumped successfully", table.TableName, station, element.name)
			}
		}
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished dump of", table.TableName)
}

type Element struct {
	name        string
	inFlagTable bool
}

func getElements(table *TableInstructions, conn *sql.DB) ([]Element, error) {
	// TODO: not sure why we only dump these two for this table
	if table.TableName == "T_HOMOGEN_MONTH" {
		return []Element{{"rr", false}, {"tam", false}}, nil
	}

	elements, err := fetchColumnNames(table.TableName, conn)
	if err != nil {
		log.Printf("Could not fetch elements for table %s: %v", table.TableName, err)
		return nil, err
	}

	// Check if element is present in flag table
	if table.FlagTableName != "" {
		flagElems, err := fetchColumnNames(table.FlagTableName, conn)
		if err != nil {
			log.Printf("Could not fetch elements for table %s: %v", table.FlagTableName, err)
			return nil, err
		}

		for i, e := range elements {
			if slices.Contains(flagElems, e) {
				elements[i].inFlagTable = true
			}
		}

		if len(elements) < len(flagElems) {
			log.Printf("WARN: Flag table %s contains more elements than Data table %s", table.TableName, table.FlagTableName)
		}
	}

	return elements, nil
}

// TODO: what's the difference between obs_origtime and klobs (they have same paramid)?
// TODO: do we need to exclude other elements?
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season"}

// Fetch column names for a given table
func fetchColumnNames(tableName string, conn *sql.DB) ([]Element, error) {
	log.Printf("Fetching elements for %s...", tableName)

	rows, err := conn.Query(
		"SELECT column_name FROM information_schema.columns WHERE table_name = $1 and NOT column_name = ANY($2::text[])",
		// NOTE: needs to be lowercase with PG
		strings.ToLower(tableName),
		INVALID_COLUMNS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var elements []Element
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		elements = append(elements, Element{name, false})
	}
	return elements, rows.Err()
}

// FIXME:? this can be extremely slow
func fetchStationNumbers(table *TableInstructions, conn *sql.DB) ([]string, error) {
	log.Println("Fetching station numbers (this can take a while)...")

	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %sL`,
		table.TableName,
	)

	if table.FlagTableName != "" {
		query = fmt.Sprintf(
			`(SELECT stnr FROM %s) UNION (SELECT stnr FROM %s)`,
			table.TableName,
			table.FlagTableName,
		)
	}

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stations := make([]string, 0)
	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}

// NOTE: inverting the loops and splitting by element does make it a bit better,
// because we avoid quering for tables that have no data or flags
func fetchStationsWithElement(table *TableInstructions, element Element, conn *sql.DB) ([]string, error) {
	log.Printf("Fetching station numbers for %s (this can take a while)...", element.name)

	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s WHERE %s IS NOT NULL`,
		table.TableName,
		element.name,
	)

	if table.FlagTableName != "" {
		if element.inFlagTable {
			query = fmt.Sprintf(
				`(SELECT stnr FROM %[2]s WHERE %[1]s IS NOT NULL) UNION (SELECT stnr FROM %[3]s WHERE %[1]s IS NOT NULL)`,
				element.name,
				table.TableName,
				table.FlagTableName,
			)
		}
	}

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stations := make([]string, 0)
	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	// log.Println(stations)
	return stations, rows.Err()
}

// Fetch min and max year from table
func fetchYearRange(tableName, station string, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string
	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = $1", tableName)

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

func dumpByYearDataOnly(args dumpFuncArgs, conn *sql.DB) error {
	begin, end, err := fetchYearRange(args.dataTable, args.station, conn)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2`,
		args.element.name,
		args.dataTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, args.station, year)
		if err != nil {
			log.Println("Could not query KDVH:", err)
			return err
		}

		path := filepath.Join(args.path, string(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Println(err)
			continue
		}

		if err := dumpToFile(path, args.element.name, rows); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func dumpByYear(args dumpFuncArgs, conn *sql.DB) error {
	if !args.element.inFlagTable {
		dumpByYearDataOnly(args, conn)
	}

	begin, end, err := fetchYearRange(args.dataTable, args.station, conn)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`SELECT
            COALESCE(d.dato, f.dato) AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, stnr, %[1]s FROM %[2]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) d
        FULL OUTER JOIN
            (SELECT dato, stnr, %[1]s FROM %[3]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) f
        ON d.dato = f.dato`,
		args.element.name,
		args.dataTable,
		args.flagTable,
	)

	flagBegin, flagEnd, err := fetchYearRange(args.flagTable, args.station, conn)
	if err != nil {
		return err
	}

	begin = min(begin, flagBegin)
	end = max(end, flagEnd)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, args.station, year)
		if err != nil {
			log.Println("Could not query KDVH:", err)
			return err
		}

		path := filepath.Join(args.path, string(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Println(err)
			continue
		}

		if err := dumpToFile(path, args.element.name, rows); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func dumpHomogenMonth(args dumpFuncArgs, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		args.element.name,
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := dumpToFile(args.path, args.element.name, rows); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func dumpDataOnly(args dumpFuncArgs, conn *sql.DB) error {
	query := fmt.Sprintf(
		"SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1",
		args.element.name,
		args.dataTable,
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := dumpToFile(args.path, args.element.name, rows); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func dumpDataAndFlags(args dumpFuncArgs, conn *sql.DB) error {
	if !args.element.inFlagTable {
		return dumpDataOnly(args, conn)
	}

	query := fmt.Sprintf(
		`SELECT
            COALESCE(d.dato, f.dato) AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
        FULL OUTER JOIN
            (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
            ON d.dato = f.dato`,
		// TODO:
		// The following query also keeps the cases where both data and flag are NULL
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
		args.element.name,
		args.dataTable,
		args.flagTable,
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := dumpToFile(args.path, args.element.name, rows); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func dumpToFile(path, element string, rows *sql.Rows) error {
	filename := filepath.Join(path, element+".csv")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	err = writeElementFile(rows, file)
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// Writes queried (time | data | flag) columns to CSV
func writeElementFile(rows *sql.Rows, file io.Writer) error {
	defer rows.Close()

	floatFormat := "%.2f"
	timeFormat := "2006-01-02_15:04:05"

	columns, err := rows.Columns()
	if err != nil {
		return errors.New("Could not ingget columns: " + err.Error())
	}

	count := len(columns)
	line := make([]string, count)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	writer := csv.NewWriter(file)
	// writer.Comma = ';'

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return errors.New("Could not scan rows: " + err.Error())
		}

		// Parse scanned types
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

			line[i] = value
		}

		if err := writer.Write(line); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return rows.Err()
}
