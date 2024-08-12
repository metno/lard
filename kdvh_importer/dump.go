package main

import (
	// TODO: maybe use sqlx?
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

type DumpArgs struct {
	Sep         string   `long:"sep" default:";"  description:"Separator character in the dumped files"`
	BaseDir     string   `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	SkipData    bool     `long:"skipdata" description:"If given, the values from dataTable will NOT be processed"`
	SkipFlags   bool     `long:"skipflag" description:"If given, the values from flagTable will NOT be processed"`
	Limit       int32    `long:"limit" default:"0" description:"If given, the procedure will stop after migrating this number of stations"`
	Overwrite   bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Combine     bool     `long:"combine" description:"Combine data and flags timeseries in a single file for import"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
}

// Populates args slices by splitting cmd strings
func (args *DumpArgs) updateConfig() {
	if args.TablesCmd != "" {
		args.Tables = strings.Split(args.TablesCmd, ",")
	}
	if args.StationsCmd != "" {
		// TODO: maybe convert to int here directly if the query requires int parsing
		args.Stations = strings.Split(args.StationsCmd, ",")
	}
	if args.ElementsCmd != "" {
		args.Elements = strings.Split(args.ElementsCmd, ",")
	}
}

// Creates the comma separated list of querySelect that we want to SELECT
func (args *DumpArgs) querySelect(tableElements []string) string {
	elements := filterSlice(args.Elements, tableElements)

	out := "dato"
	// TODO: should `e` be converted to lowercase?
	for _, e := range elements {
		out += fmt.Sprintf(",%v", e)
	}
	return out
}

func getDB(connString string) *sql.DB {
	connector := go_ora.NewConnector(connString)
	conn := sql.OpenDB(connector)

	// NOTE: this in theory should last for the whole connection
	if _, err := conn.Exec("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD_HH24:MI:SS'"); err != nil {
		log.Fatalln(err)
	}
	return conn

}

func (args *DumpArgs) Execute(_ []string) error {
	args.updateConfig()

	dvhConn := getDB(os.Getenv("DVH_STRING"))
	klima11Conn := getDB(os.Getenv("KLIMA11_STRING"))

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for _, table := range TABLE2INSTRUCTIONS {
		if args.Tables != nil && !slices.Contains(args.Tables, table.TableName) {
			continue
		}

		if table.FromKlima11 {
			processTable(table, klima11Conn, args)
		} else {
			processTable(table, dvhConn, args)
		}

		combineTables(table, args)
	}

	return nil
}

func processTable(table *TableInstructions, conn *sql.DB, args *DumpArgs) {
	if !args.SkipData {
		dumpTable(table.TableName, table.SplitQuery, conn, args)
	}

	if !args.SkipFlags || table.FlagTableName != "" {
		dumpTable(table.FlagTableName, table.SplitQuery, conn, args)
	}
}

func dumpTable(tableName string, fetchByYear bool, conn *sql.DB, config *DumpArgs) {
	defer sendEmailOnPanic("dumpData", config.Email)

	outdir := filepath.Join(config.BaseDir, tableName)
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		log.Println("Skipping data dump of", tableName, "because dumped folder already exists")
		return
	}

	log.Println("Starting data dump of", tableName)
	setLogFile(tableName, "dump")

	stations, err := fetchStationNumbers(tableName, conn)
	if err != nil {
		log.Printf("Could not fetch stations for table %s: %v", tableName, err)
		return
	}

	columns, err := fetchColumnNames(tableName, conn)
	if err != nil {
		log.Printf("Could not fetch column names for table %s: %v", tableName, err)
		return
	}

	stations = filterSlice(config.Stations, stations)
	for _, station := range stations {
		// TODO: don't know if the queries will work with int64
		// TODO: do I need int64?
		stnr, err := strconv.ParseInt(station, 10, 32)
		if err != nil {
			log.Println("Could not parse station number:", err)
			continue
		}

		if fetchByYear {
			if err := dumpByYear(tableName, stnr, columns, conn, config); err != nil {
				log.Println("Could not dump data:", err)
				continue
			}
		} else {
			if err := dump(tableName, stnr, columns, conn, config); err != nil {
				log.Println("Could not dump data:", err)
				continue
			}
		}
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished data dump of", tableName)
}

// Fetch column names for a given table
func fetchColumnNames(tableName string, conn *sql.DB) ([]string, error) {
	// TODO: do I need DISTINCT?
	rows, err := conn.Query(
		"SELECT column_name FROM all_tab_columns WHERE table_name = :1",
		tableName,
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

// TODO: confirm return / query type
func fetchStationNumbers(tableName string, conn *sql.DB) ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT stnr FROM %s ORDER BY stnr", tableName)
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
	return stations, nil
}

func dump(tableName string, station int64, columns []string, conn *sql.DB, config *DumpArgs) error {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE stnr = :1 ORDER BY dato", config.querySelect(columns), tableName)

	// hack for T_HOMOGEN_MONTH, to single out the month values
	if tableName == "T_HOMOGEN_MONTH" {
		query = "SELECT dato,tam,rr FROM T_HOMOGEN_MONTH WHERE stnr = :1 AND SEASON BETWEEN 1 AND 12 ORDER BY dato"
	}

	rows, err := conn.Query(query, station)
	if err != nil {
		log.Println("Could not query KDVH:", err)
		return err
	}

	p := filepath.Join(config.BaseDir, tableName, string(station))
	if err = writeElementFiles(rows, p, config.Sep); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// Fetch min and max year from table
func fetchYearRange(tableName string, station int64, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string

	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = :1", tableName)
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

func dumpByYear(tableName string, station int64, columns []string, conn *sql.DB, config *DumpArgs) error {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE stnr = :1 AND TO_CHAR(dato, 'yyyy') = :2 ORDER BY dato",
		config.querySelect(columns),
		tableName,
	)

	begin, end, err := fetchYearRange(tableName, station, conn)
	if err != nil {
		return err
	}

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, station, string(year))
		if err != nil {
			log.Println("Could not query KDVH:", err)
			return err
		}

		p := filepath.Join(config.BaseDir, tableName, string(station), string(year))
		if err = writeElementFiles(rows, p, config.Sep); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

// Writes each column in the queried table to separate files
func writeElementFiles(rows *sql.Rows, path string, sep string) error {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return errors.New("Could not get columns: " + err.Error())
	}

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return errors.New("Could not create directory: " + err.Error())
	}

	count := len(columns)
	files := make([]*os.File, count-1)

	for i := range files {
		filename := filepath.Join(path, columns[i+1]+".csv")
		file, err := os.Create(filename)
		if err != nil {
			return errors.New(fmt.Sprintf("Could not create file '%s': %s", filename, err))
		}
		defer file.Close()

		files[i] = file
	}

	// TODO: need to write headers first?
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
			default:
				value = fmt.Sprintf("%v", v)
			}

			values[i] = value
		}

		// Write to files
		for i := range values[1:] {
			line := fmt.Sprintf("%s%s%s\n", values[0], sep, values[i+1])

			if _, err := files[i].WriteString(line); err != nil {
				return errors.New("Could not write to file: " + err.Error())
			}
		}
	}
	return nil
}

func combineTables(table *TableInstructions, config *DumpArgs) error {
	if !config.Combine {
		// log.Println("Skipping combine step")
		return nil
	}

	outdir := filepath.Join(config.BaseDir, table.TableName+"_combined")
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		log.Println("Skipping combine step of", table.TableName, "because folder already exists")
		return nil
	}

	// TODO: need to filter?
	for _, station := range config.Stations {

		// TODO: check file mode
		stationdir := filepath.Join(outdir, station)
		err := os.MkdirAll(stationdir, os.ModePerm)
		if err != nil {
			log.Println("Could not create directory:", err)
			return err
		}

		for _, element := range config.Elements {
			dataFile := filepath.Join(config.BaseDir, table.TableName, station, element+".csv")
			data, err := readFile(dataFile)
			if err != nil {
				log.Printf("Could not read data file '%s': %s", dataFile, err)
				continue
			}

			var flags [][]string
			if table.FlagTableName != "" {
				flagFile := filepath.Join(config.BaseDir, table.FlagTableName, station, element+".csv")
				flags, err = readFile(flagFile)
				if err != nil {
					log.Printf("Could not read flag file '%s': %s", flagFile, err)
					continue
				}

				if len(data) != len(flags) {
					log.Printf("Different number of rows (%v vs %v)\n", len(data), len(flags))
					continue
				}

				if len(data[0]) != len(flags[0]) {
					log.Printf("Different number of columns (%v vs %v)\n", len(data[0]), len(flags[0]))
					continue
				}
			}

			outfile := filepath.Join(stationdir, element+".csv")
			if err := writeCombined(data, flags, outfile, config.Sep); err != nil {
				log.Println(err)
				continue
			}
		}
	}
	return nil
}

// write a new file using data (and optionally flag) files with format "timestamp<sep>data<sep>(flag)"
func writeCombined(data [][]string, flags [][]string, filename string, sep string) error {
	outfile, err := os.Create(filename)
	if err != nil {
		return errors.New(fmt.Sprintf("Could not open file '%s': %s\n", filename, err))
	}
	defer outfile.Close()

	for i := range data {
		line := fmt.Sprintf("%s%s%s%s", data[i][0], sep, data[i][1], sep)
		if flags != nil {
			if flags[i][0] != data[i][0] {
				return errors.New("Different timestamps in data and flag files")
			}
			line += flags[i][1]
		}

		if _, err := outfile.WriteString(line + "\n"); err != nil {
			return errors.New(fmt.Sprintf("Could not write to file '%s': %s\n", filename, err))
		}
	}
	return nil
}
