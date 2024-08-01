package main

import (
	// TODO: maybe use sqlx?
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

type DumpArgs struct {
	BaseDir   string `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep       string `long:"sep" default:";"  description:"Separator character in the dumped files"`
	Tables    string `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations  string `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements  string `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	SkipData  bool   `long:"skipdata" description:"If given, the values from dataTable will NOT be processed"`
	SkipFlags bool   `long:"skipflag" description:"If given, the values from flagTable will NOT be processed"`
	Limit     int32  `long:"limit" default:0 description:"If given, the procedure will stop after migrating this number of stations"`
	Overwrite bool   `long:"overwrite" description:"Overwrite any existing dumped files"`
}

func (self *DumpArgs) Execute(args []string) error {
	// defer EmailOnPanic("dumpData")

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

	for name, table := range TABLE2INSTRUCTIONS {
		if self.Tables != "" && !strings.Contains(self.Tables, name) {
			continue
		}
		// TODO: should be safe to spawn goroutines/waitgroup here with connection pool
		// TODO: are these the same for data and flags table?
		// also useless here?
		fetchColumnNames(table.TableName, conn)

		// TODO: are these the same for data and flags table?
		if self.Stations == "" {
			// TODO: this should modify self.Stations
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

	for _, station := range strings.Split(args.Stations, ",") {
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

		// TODO:
		// 4. write columns
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished data dump of", tableName)
}

// TODO: remove, it's useless
func fetchColumnNames(tableName string, conn *sql.DB) {
	rows, err := conn.Query("SELECT column_id, column_name FROM all_tab_columns WHERE table_name = :1", tableName)
	if err != nil {
		return
	}
	defer rows.Close()

	//TODO: scan rows
}

func fetchStationNumbers(tableName string, conn *sql.DB) {
	rows, err := conn.Query("SELECT DISTINCT stnr FROM :1 ORDER BY stnr", tableName)
	if err != nil {
		return
	}
	defer rows.Close()

	//TODO: scan rows
}

func fetchData(tableName string, station int64, conn *sql.DB, args *DumpArgs) {
	query := "SELECT * FROM :1 WHERE stnr = :2"
	if tableName == "T_HOMOGEN_MONTH" {
		// hack for T_HOMOGEN_MONTH, to single out the month values
		query = "SELECT stnr,dato,tam,rr FROM :1 WHERE stnr = :2 AND SEASON BETWEEN 1 AND 12"
	} else if args.Elements != "" {
		query = fmt.Sprintf("SELECT stnr,dato,%s FROM :1 WHERE stnr = :2 AND SEASON BETWEEN 1 AND 12", args.Elements)
	}

	rows, err := conn.Query(query, tableName, station)
	if err != nil {
		return
	}
	defer rows.Close()

	// TODO: handle unknown columns
	// cols, err := rows.Columns()
	// if err != nil {
	// 	return
	// }
	// element columns
	// elements := make([]string, len(cols) - 2)

	//TODO: scan rows

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
		return 0, 0, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return begin, end, nil
}

func fetchDataByYear(tableName string, station int64, conn *sql.DB, args *DumpArgs) {
	query := "SELECT * FROM :1 WHERE stnr = :2 AND TO_CHAR(dato, 'yyyy') = :3"
	if args.Elements != "" {
		query = fmt.Sprintf("SELECT stnr,dato,%s FROM :1 WHERE stnr = :2 AND TO_CHAR(dato, 'yyyy') = :3", args.Elements)
	}

	begin, end, err := fetchYearRange(tableName, station, conn)
	if err != nil {
		// TODO: handle error
		return
	}

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, tableName, station, string(year))
		if err != nil {
			return
		}

		// TODO: scan rows
		rows.Close()
	}
}
