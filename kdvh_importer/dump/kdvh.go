package dump

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"kdvh_importer/utils"
)

// KDVHTable contain metadata on how to treat different tables in KDVH
type KDVHTable struct {
	TableName     string            // Name of the table with observations
	FlagTableName string            // Name of the table with QC flags for observations
	DumpFunc      TableDumpFunction //
}

func newKDVHTable(args ...string) *KDVHTable {
	if len(args) > 2 {
		panic("This function only accepts two arguments")
	}

	dumpFunc := dumpDataOnly
	var flagTableName string

	if len(args) == 2 {
		dumpFunc = dumpDataAndFlags
		flagTableName = args[1]
	}

	return &KDVHTable{
		TableName:     args[0],
		FlagTableName: flagTableName,
		DumpFunc:      dumpFunc,
	}
}

// List of all the tables we care about
var KDVH_TABLES = []*KDVHTable{
	newKDVHTable("T_EDATA", "T_EFLAG"),
	newKDVHTable("T_METARDATA"),
	newKDVHTable("T_ADATA", "T_AFLAG"),
	newKDVHTable("T_MDATA", "T_MFLAG"),
	newKDVHTable("T_TJ_DATA", "T_TJ_FLAG"),
	newKDVHTable("T_PDATA", "T_PFLAG"),
	newKDVHTable("T_NDATA", "T_NFLAG"),
	newKDVHTable("T_VDATA", "T_VFLAG"),
	newKDVHTable("T_UTLANDDATA", "T_UTLANDFLAG"),
	newKDVHTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL"),
	newKDVHTable("T_DIURNAL", "T_DIURNAL_FLAG"),
	newKDVHTable("T_AVINOR", "T_AVINOR_FLAG"),
	newKDVHTable("T_PROJDATA", "T_PROJFLAG"),
	newKDVHTable("T_CDCV_DATA", "T_CDCV_FLAG"),
	newKDVHTable("T_MERMAID", "T_MERMAID_FLAG"),
	newKDVHTable("T_SVVDATA", "T_SVVFLAG"),
	newKDVHTable("T_MONTH", "T_MONTH_FLAG"),
	newKDVHTable("T_HOMOGEN_DIURNAL"),
	{TableName: "T_10MINUTE_DATA", FlagTableName: "T_10MINUTE_FLAG", DumpFunc: dumpByYear},
	{TableName: "T_MINUTE_DATA", FlagTableName: "T_MINUTE_FLAG", DumpFunc: dumpByYear},
	{TableName: "T_SECOND_DATA", FlagTableName: "T_SECOND_FLAG", DumpFunc: dumpByYear},
	{TableName: "T_HOMOGEN_MONTH", DumpFunc: dumpHomogenMonth},
}

type Element struct {
	name        string
	inFlagTable bool
}
type Elements []Element

func (s *Elements) get(v string) (*Element, bool) {
	for _, element := range *s {
		if v == element.name {
			return &element, true
		}
	}
	return nil, false
}

func filterElements(slice []string, reference Elements) []Element {
	if slice == nil {
		return reference
	}

	out := make([]Element, len(slice))
	for _, e := range slice {
		elem, ok := reference.get(e)
		if !ok {
			slog.Warn(fmt.Sprintf("Element '%s' not present in database", e))
			continue
		}
		out = append(out, *elem)
	}

	return nil
}

func (table *KDVHTable) dump(conn *sql.DB, config *DumpConfig) {
	defer utils.SendEmailOnPanic("kdvh table.dump", config.Email)

	// TODO: should probably do it at the station/element level?
	outdir := filepath.Join(config.BaseDir, table.TableName+"_combined")
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		slog.Info(fmt.Sprint("Skipping data dump of ", table.TableName, " because dumped folder already exists"))
		return
	}

	slog.Info(fmt.Sprint("Starting dump of ", table.TableName))
	utils.SetLogFile(table.TableName, "dump")
	elements, err := getElements(table, conn, config)
	if err != nil {
		return
	}

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for _, element := range elements {
		stations, err := getStationsWithElement(element, table, conn, config)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not fetch stations for table %s: %v", table.TableName, err))
			continue
		}

		for _, station := range stations {
			path := filepath.Join(outdir, string(station))
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				slog.Error(err.Error())
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

			// NOTE: Non-nil errors are logged inside each DumpFunc
			if err == nil {
				slog.Info(fmt.Sprintf("%s - %s - %s: dumped successfully", table.TableName, station, element.name))
			}
		}
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished dump of", table.TableName)
}

func getElements(table *KDVHTable, conn *sql.DB, config *DumpConfig) ([]Element, error) {
	elements, err := table.fetchElements(conn)
	if err != nil {
		return nil, err
	}

	elements = filterElements(config.Elements, elements)
	return elements, nil
}

func getStationsWithElement(element Element, table *KDVHTable, conn *sql.DB, config *DumpConfig) ([]string, error) {
	stations, err := table.fetchStationsWithElement(element, conn)
	if err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Element '%s'", element.name) + "not available for station '%s'"
	stations = utils.FilterSlice(config.Stations, stations, msg)
	return stations, nil
}

func (table *KDVHTable) fetchElements(conn *sql.DB) ([]Element, error) {
	// TODO: not sure why we only dump these two for this table
	// TODO: separate this to its own function? Separate edge cases
	if table.TableName == "T_HOMOGEN_MONTH" {
		return []Element{{"rr", false}, {"tam", false}}, nil
	}

	elements, err := fetchColumnNames(table.TableName, conn)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
		return nil, err
	}

	// Check if element is present in flag table
	// NOTE: For example, unknown element 'xxx' (which is an empty column) in table T_TJ_DATA is missing from T_TJ_FLAG
	// TODO: probably should not fetch 'xxx' anyway since it's not in Stinfosys anyway, and simply log
	// if the element is not in the flag table? Because this feels like another edge case
	if table.FlagTableName != "" {
		flagElems, err := fetchColumnNames(table.FlagTableName, conn)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.FlagTableName, err))
			return nil, err
		}

		for i, e := range elements {
			if slices.Contains(flagElems, e) {
				elements[i].inFlagTable = true
			}
		}

		if len(elements) < len(flagElems) {
			slog.Warn(fmt.Sprintf("%s contains more elements than %s", table.FlagTableName, table.TableName))
		}
	}

	return elements, nil
}

// List of columns that are not selected in KDVH queries
// TODO: what's the difference between obs_origtime and klobs (they have same paramid)?
// Should they be added here? Do we need to exclude other elements?
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season"}

// Fetch column names for a given table
func fetchColumnNames(tableName string, conn *sql.DB) ([]Element, error) {
	slog.Info(fmt.Sprintf("Fetching elements for %s...", tableName))

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

func fetchStationNumbers(table *KDVHTable, conn *sql.DB) ([]string, error) {
	slog.Info(fmt.Sprint("Fetching station numbers (this can take a while)..."))

	// FIXME:? this can be extremely slow
	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s`,
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
// because we avoid quering for tables that have no data or flag for that element
func (table *KDVHTable) fetchStationsWithElement(element Element, conn *sql.DB) ([]string, error) {
	slog.Info(fmt.Sprintf("Fetching station numbers for %s (this can take a while)...", element.name))

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
