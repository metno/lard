package main

import (
	"bufio"
	"context"

	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/rickb777/period"
	// "github.com/jackc/pgx/v5/pgxpool"
)

const (
	kdvhLabel     = `{"TableName":"%s", "ElemCode":"%s", "Stnr":%d}`
	kdvhLabelFlag = `{"TableName":"%s", "ElemCode":"%s", "Stnr":%d, "FlagTable":"%s"}`
	gRPCRetries   = 10
	sleepTime     = 15
	batchSize     = 300
	NUMWORKERS    = 10
)

// TODO: define the schema we want to export here
// And probably we should use nullable types?
type Observation struct {
	// Epoch             int64     // Truncated second count since 1970-01-01 00:00:00. Updated every EpochDuration

	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time

	// Fromtime          time.Time // Fromtime of measurement/aggregation interval
	// Totime            time.Time // Totime of measurement/aggregation interval
	// StdDev            float64   // Standard deviation of aggregation
	// Count             int32     // Input data value count for aggregation
	// Lat               float32   // Latitude in degrees north
	// Lon               float32   // Longitude in degrees east
	// HAMSL             float32   // Height above mean sea level in meters
	// HAG               float32   // Height above ground in meters

	// Observation data formatted as a double precision floating point
	Data float64
	// Observation data formatted as a binary large object.
	DataBlob []byte

	// KVCorrQC1         float64   // Estimated observation data from Quality Control-stage 1
	// KVCorrQC2         float64   // Estimated observation data from Quality Control-stage 2
	// CorrHQC           float64   // Estimated observation data from Human Quality Control

	// NOTE(manuel): unknown method ??????????
	// Estimated observation data from KDVH (unknown method)
	CorrKDVH float64
	// Latest updated corrected observation data value
	Corrected float64
	// Flag encoding quality control status
	KVFlagControlInfo []byte
	// Flag encoding quality control status
	KVFlagUseInfo []byte
	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
	KDVHFlag []byte
	// Comma separated value listing checks that failed during quality control
	KVCheckFailed string
}

// DataPageFunction is a function that creates a properly formatted TimeSeriesData object
type DataPageFunction func(KDVHData) (Observation, error)

type KDVHData struct {
	ID       int32
	elemCode string
	obsTime  time.Time
	data     string
	flags    string
	offset   period.Period
}

// TableInstructions contain metadata on how to treat different tables in KDVH
type TableInstructions struct {
	TableName          string
	FlagTableName      string
	ElemTableName      string
	CustomDataFunction DataPageFunction
	ImportUntil        int64 // stop when reaching this year
	FromKlima11        bool  // dump from klima11 not dvh10
	SplitQuery         bool  // split dump queries into yearly batches
}

// KDVHKey contains the important keys from the fetchkdvh labeltype
type KDVHKey struct {
	Stnr int64 `json:"Stnr"`
	ParamKey
}

// ParamKey is a subset of KDVHKeys used for lookup of parameter offsets
type ParamKey struct {
	ElemCode  string `json:"ElemCode"`
	TableName string `json:"TableName"`
}

// TODO: this would be queried directly from KDVH?
// better than querying stinfosys?
type TimeSeriesInfoKDVH struct {
	Stnr     int64      `db:"stnr"`
	ElemCode string     `db:"elem_code"`
	FromTime *time.Time `db:"fdato"`
	ToTime   *time.Time `db:"tdato"`
}

// TODO: these should be pointers or pgx/sql nulltypes?
type ElemMap struct {
	ElemCode  string
	TableName string
	TypeID    int32
	ParamID   int32
	Hlevel    int32
	Sensor    int32
	Fromtime  time.Time
	// totime    *time.Time
}

type StinfoChannels struct {
	keys chan KDVHKey
	meta chan Result[ElemMap]
}

func NewStinfoChannels() *StinfoChannels {
	keys := make(chan KDVHKey, 10)
	meta := make(chan Result[ElemMap], 10)
	return &StinfoChannels{keys, meta}
}

// TODO: why are these even here in the first place?
func isNotValid(elemCode string) bool {
	return (len(elemCode) > 4 && (elemCode[0:4] == "KOPI" || elemCode[len(elemCode)-4:] == "KOPI")) ||
		elemCode == "TYPEID" ||
		elemCode == "TAM_NORMAL_9120" ||
		elemCode == "RRA_NORMAL_9120" ||
		elemCode == "OT" ||
		elemCode == "OTN" ||
		elemCode == "OTX" ||
		elemCode == "DD06" ||
		elemCode == "DD12" ||
		elemCode == "DD18"

}

type MigrationConfig struct {
	BaseDir      string
	DoAll        bool
	Sep          string
	Tables       []string
	Stations     []string
	Elements     []string
	ParamOffsets map[ParamKey]period.Period
}

func NewMigrationConfig(args *CmdArgs) *MigrationConfig {
	var tables, stations, elements []string

	if args.DataTable != "" {
		tables = strings.Split(args.DataTable, ",")
	}
	if args.StnrList != "" {
		stations = strings.Split(args.StnrList, ",")
	}
	if args.ElemCodeList != "" {
		elements = strings.Split(args.ElemCodeList, ",")
	}

	ParamOffsets, err := cacheParamOffsets()
	if err != nil {
		log.Fatalln("Could not load param offsets", err)
	}

	return &MigrationConfig{
		BaseDir:      args.BaseDir,
		Sep:          args.Sep,
		Tables:       tables,
		Stations:     stations,
		Elements:     elements,
		ParamOffsets: ParamOffsets,
	}
}

// define how to treat different tables
var TABLE2INSTRUCTIONS = map[string]*TableInstructions{
	// unique tables imported in their entirety
	"T_EDATA":                {TableName: "T_EDATA", FlagTableName: "T_EFLAG", ElemTableName: "T_ELEM_EDATA", CustomDataFunction: makeDataPageEdata, ImportUntil: 3001},
	"T_METARDATA":            {TableName: "T_METARDATA", ElemTableName: "T_ELEM_METARDATA", CustomDataFunction: makeDataPage, ImportUntil: 3000},
	"T_DIURNAL_INTERPOLATED": {TableName: "T_DIURNAL_INTERPOLATED", CustomDataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	"T_MONTH_INTERPOLATED":   {TableName: "T_MONTH_INTERPOLATED", CustomDataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	// tables with some data in kvalobs, import only up to 2005-12-31
	"T_ADATA":      {TableName: "T_ADATA", FlagTableName: "T_AFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 2006},
	"T_MDATA":      {TableName: "T_MDATA", FlagTableName: "T_MFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 2006},
	"T_TJ_DATA":    {TableName: "T_TJ_DATA", FlagTableName: "T_TJ_FLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 2006},
	"T_PDATA":      {TableName: "T_PDATA", FlagTableName: "T_PFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPagePdata, ImportUntil: 3000},
	"T_NDATA":      {TableName: "T_NDATA", FlagTableName: "T_NFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPageNdata, ImportUntil: 2006},
	"T_VDATA":      {TableName: "T_VDATA", FlagTableName: "T_VFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPageVdata, ImportUntil: 2006},
	"T_UTLANDDATA": {TableName: "T_UTLANDDATA", FlagTableName: "T_UTLANDFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 2006},
	// tables that should only be dumped
	"T_10MINUTE_DATA": {TableName: "T_10MINUTE_DATA", FlagTableName: "T_10MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, SplitQuery: true},
	"T_MINUTE_DATA":   {TableName: "T_MINUTE_DATA", FlagTableName: "T_MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, SplitQuery: true},
	"T_SECOND_DATA":   {TableName: "T_SECOND_DATA", FlagTableName: "T_SECOND_FLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, SplitQuery: true},
	"T_ADATA_LEVEL":   {TableName: "T_ADATA_LEVEL", FlagTableName: "T_AFLAG_LEVEL", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage},
	"T_CDCV_DATA":     {TableName: "T_CDCV_DATA", FlagTableName: "T_CDCV_FLAG", ElemTableName: "T_ELEM_EDATA", CustomDataFunction: makeDataPage},
	"T_MERMAID":       {TableName: "T_MERMAID", FlagTableName: "T_MERMAID_FLAG", ElemTableName: "T_ELEM_EDATA", CustomDataFunction: makeDataPage},
	"T_DIURNAL":       {TableName: "T_DIURNAL", FlagTableName: "T_DIURNAL_FLAG", ElemTableName: "T_ELEM_DIURNAL", CustomDataFunction: makeDataPageProduct, ImportUntil: 2006},
	"T_AVINOR":        {TableName: "T_AVINOR", FlagTableName: "T_AVINOR_FLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, FromKlima11: true},
	"T_SVVDATA":       {TableName: "T_SVVDATA", FlagTableName: "T_SVVFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage},
	"T_PROJDATA":      {TableName: "T_PROJDATA", FlagTableName: "T_PROJFLAG", ElemTableName: "T_ELEM_PROJ", CustomDataFunction: makeDataPage, FromKlima11: true},
	// other special cases
	"T_MONTH":           {TableName: "T_MONTH", FlagTableName: "T_MONTH_FLAG", ElemTableName: "T_ELEM_MONTH", CustomDataFunction: makeDataPageProduct, ImportUntil: 1957},
	"T_HOMOGEN_DIURNAL": {TableName: "T_HOMOGEN_DIURNAL", ElemTableName: "T_ELEM_HOMOGEN_MONTH", CustomDataFunction: makeDataPageProduct},
	"T_HOMOGEN_MONTH":   {TableName: "T_HOMOGEN_MONTH", ElemTableName: "T_ELEM_HOMOGEN_MONTH", CustomDataFunction: makeDataPageProduct},
	// metadata notes for other tables
	// "T_SEASON": {ElemTableName: "T_SEASON"},
}

func migrationStep(config *MigrationConfig, step func(t *TableInstructions, m *MigrationConfig)) {
	for name, ti := range TABLE2INSTRUCTIONS {
		// Skip tables not in table list
		if config.Tables != nil && !slices.Contains(config.Tables, name) {
			continue
		}
		step(ti, config)
	}
}

func importTable(ti *TableInstructions, config *MigrationConfig) {
	// send an email if something panics inside here
	// defer EmailOnPanic("importTable")

	if ti.ImportUntil == 0 {
		log.Printf("Skipping import of %s because this table is not set for import\n", ti.TableName)
		return
	}

	// Connetct to LARD database
	conn, err := pgx.Connect(context.Background(), os.Getenv("LARD_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database: ", err)
	}

	log.Println("Starting import of", ti.TableName)
	setLogFile(ti.TableName, "import")

	// Spawn STINFO database connection in separate task
	stchan := NewStinfoChannels()
	go QueryStinfosys(stchan)

	// Get station dirs from table directory
	importPath := fmt.Sprintf("%s%s_combined", config.BaseDir, ti.TableName)
	stations, err := os.ReadDir(importPath)
	if err != nil {
		log.Println("importTable os.ReadDir on", importPath, "-", err)
		return
	}

	// loop over stations found in the directory
	for _, station := range stations {
		parseStation(importPath, station, conn, stchan, ti, config)
	}

	log.SetOutput(os.Stdout)
	if ti.FlagTableName != "" {
		log.Println("Finished data and flag import of", ti.TableName)
	} else {
		log.Println("Finished data import of", ti.TableName)
	}
}

func parseStation(path string, station os.DirEntry, conn *pgx.Conn, stchan *StinfoChannels, ti *TableInstructions, config *MigrationConfig) {
	if !station.IsDir() {
		return
	}

	// skip if station is not in the given list
	if config.Stations != nil && !slices.Contains(config.Stations, station.Name()) {
		return
	}

	// check station directory
	stationDir := fmt.Sprintf("%s/%s", path, station.Name())
	elemFiles, err := os.ReadDir(stationDir)
	if err != nil {
		log.Println("importTable - os.ReadDir on", stationDir, "-", err)
		return
	}

	// parse the station number
	stnr, err := strconv.ParseInt(station.Name(), 10, 64)
	if err != nil {
		log.Println("importTable - strconv.ParseInt -", err)
		return
	}

	for _, element := range elemFiles {
		parseElement(stationDir, element, stnr, conn, stchan, config, ti)
	}

}

func parseElement(path string, element os.DirEntry, stnr int64, conn *pgx.Conn, stinfo *StinfoChannels, config *MigrationConfig, ti *TableInstructions) {
	elemCode := strings.TrimSuffix(element.Name(), ".csv")

	// skip if element is not in the given list
	if config.Elements != nil && !slices.Contains(config.Elements, elemCode) {
		return
	}

	// check that this elemCode is valid and desireable (whatever that means)
	if isNotValid(elemCode) {
		log.Println("Skipped", elemCode)
		return
	}

	// find any offset info, returns 0 offset if not in the map
	offset := config.ParamOffsets[ParamKey{elemCode, ti.TableName}]

	// open file
	filename := fmt.Sprintf("%s/%s", path, element.Name())
	fh, err := os.Open(filename)
	if err != nil {
		SendEmail("ODA - KDVH importer worker crashed", "In importTimeSeriesWorker os.Open:\n"+err.Error())
		log.Fatalln("importStationData os.Open -", err)
	}

	// setup line scanner
	scanner := bufio.NewScanner(fh)

	// check header row: maybe it's semicolon, maybe it's comma! :(
	cols := make([]string, 2)
	scanner.Scan()

	cols = strings.Split(scanner.Text(), config.Sep)
	if len(cols) < 2 {
		err = fh.Close()
		if err != nil {
			log.Println("Error when closing file:", err)
		}
		log.Println("First line of file was:", scanner.Text(), "- was the separator wrong?")
		log.Println("Skipped", ti.TableName, elemCode, stnr, ti.FlagTableName)
		return
	}

	// use relevant fromtime and totime from ElemTableName to avoid importing nonsense flags
	// TODO: what does this mean?
	// kdvhinfo, ok := TableTimeSeriesKDVH[fi.KDVHKeys]
	// if !ok {
	// that is okay though, orig_obstime is not defined in elem tables
	// TODO: again what does this mean?
	// log.Println("TimeSeries not recognized in KDVH cache", fi.KDVHKeys)
	// }

	// query timeseries id from lard
	tid, err := getTimeSeriesID(KDVHKey{stnr, ParamKey{ti.TableName, elemCode}}, conn, stinfo)
	if err != nil {
		log.Println("Error on obtaining timeseries id:", err)
	}

	data := parseObs(TsMeta{tid, elemCode, offset}, scanner, ti, config)

	count, err := insertData(conn, data)
	if err != nil {
		log.Println("Failed bulk insertion", err)
	}

	log.Printf("%v - station %v - element %v: inserted %v rows\n", ti.TableName, stnr, elemCode, count)
	if int(count) != len(data) {
		log.Printf("WARN: Not all rows have been inserted (%v/%v)\n", count, len(data))
	}
}

type TsMeta struct {
	ID       int32
	elemCode string
	offset   period.Period
}

func parseObs(meta TsMeta, scanner *bufio.Scanner, ti *TableInstructions, config *MigrationConfig) []Observation {
	data := make([]Observation, 0, 5000)

	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			log.Println("Error parsing time:", err)
			continue
		}

		// TODO: so we actually need to either query KDVH directly or the proxy?
		// only import data between kdvh's defined fromtime and totime
		// if kdvhinfo != nil { if kdvhinfo.FromTime != nil && obsTime.Sub(*kdvhinfo.FromTime) < 0 { continue }
		// if kdvhinfo.ToTime != nil && obsTime.Sub(*kdvhinfo.ToTime) > 0 { break } }

		// stop importing when reached ImportUntil
		if obsTime.Year() >= int(ti.ImportUntil) {
			log.Println("Reached max import time for this table, inserting data")
			break
		}

		tempdata, err := ti.CustomDataFunction(
			KDVHData{
				ID:       meta.ID,
				elemCode: meta.elemCode,
				offset:   meta.offset,
				obsTime:  obsTime,
				data:     cols[1],
				flags:    cols[2],
			},
		)

		if err != nil {
			log.Println("dataPage -", err)
			continue
		}
		data = append(data, tempdata)
	}

	return data
}

func insertData(conn *pgx.Conn, data []Observation) (int64, error) {
	// TODO: should this be a transaction?
	return conn.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "corrected", "controlinfo", "useinfo"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				data[i].CorrKDVH,
				data[i].KVFlagControlInfo,
				data[i].KVFlagUseInfo,
			}, nil
		}),
	)
}

// background task that receives kdvh keys and returns timeseries metadata information
func QueryStinfosys(stchan *StinfoChannels) {
	conn, err := pgx.Connect(context.TODO(), os.Getenv("STINFO_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database:", err)
	}

	// NOTE: time_series_kdvh does not have all KDVH table names, while elem_map_cfnames_param seem to have all of them
	// but it's missing the totime column
	// TODO: Make sure this is the correct table to query
	for key := range stchan.keys {
		rows, err := conn.Query(context.TODO(),
			`SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime FROM elem_map_cfnames_param
        WHERE table_name = $1
        AND elem_code = $2`,
			// AND stationid = $3
			&key.TableName, &key.ElemCode)

		if err != nil {
			stchan.meta <- Err[ElemMap](err)
			continue
		}

		result, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[ElemMap])
		if err != nil {
			rows.Close()
			stchan.meta <- Err[ElemMap](err)
			continue
		}

		rows.Close()
		stchan.meta <- Ok(result)
	}
}

func getTimeSeriesID(key KDVHKey, conn *pgx.Conn, stinfo *StinfoChannels) (int32, error) {
	var tsid int32

	// Send timeseries key to channel
	stinfo.keys <- key

	// Receive metadata result from stinfosys task
	result := <-stinfo.meta
	if result.Err != nil {
		// TODO: missing entry in Stinfo for that KDVHkey
		log.Println("Missing entry in Stinfosys")
		return tsid, result.Err
	}

	// Query LARD labels table with stinfosys metadata
	meta := result.Ok
	err := conn.QueryRow(context.TODO(), `SELECT timeseries FROM labels.met
      WHERE station_id = $1
      AND param_id = $2
      AND type_id = $3
      AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
      AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		&key.Stnr, &meta.ParamID, &meta.TypeID, &meta.Hlevel, &meta.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID
	if err == nil {
		return tsid, nil
	}

	// Otherwise insert timeseries if not present in the lard DB
	transaction, err := conn.Begin(context.TODO())
	if err != nil {
		return tsid, err
	}

	err = transaction.QueryRow(
		context.TODO(),
		// `INSERT INTO public.timeseries (fromtime, totime) VALUES ($1, $2) RETURNING id`,
		`INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id`,
		meta.Fromtime,
		// meta.totime,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(context.TODO(), `INSERT INTO labels.met
      (timeseries, station_id, param_id, type_id, lvl, sensor)
      VALUES ($1, $2, $3, $4, $5, $6)`,
		&tsid, &key.Stnr, &meta.ParamID, &meta.TypeID, &meta.Hlevel, &meta.Sensor)
	if err != nil {
		return tsid, err
	}

	err = transaction.Commit(context.TODO())
	if err != nil {
		return tsid, err
	}

	// TODO: I guess we don't care about this conversion?
	// relabel to kvkafka if wanted and if possible
	// if ti.ImportUntil > 0 {
	// 	kvkafka, err := relabelKdvh2Kvkafka(ti, keys, resp.GetID(), s)
	// 	if err != nil {
	// 		log.Println("Could not relabel", label, "to", kvkafka, "-", err)
	// 	}
	// }

	return tsid, nil
}

// relabel from kdvh to kvkafka
// func relabelKdvh2Kvkafka(ti *TableInstructions, keys KDVHKeys, tsid int64, s *ODASession) (label string, err error) {
// 	// transform
// 	kvkafka, err := keytransform.KDVH2Kvkafka(keytransform.KeysKDVH{
// 		Stnr:      int(keys.Stnr),
// 		ElemCode:  keys.ElemCode,
// 		TableName: keys.TableName,
// 	})
// 	if err != nil {
// 		return label, err
// 	}
// 	// marshal
// 	labelbyte, err := json.Marshal(kvkafka)
// 	label = string(labelbyte)
// 	if err != nil {
// 		return label, err
// 	}
// 	// label
// 	ctx, cancel := NewContext(60)
// 	defer cancel()
// 	_, err = s.conn.LabelTimeSeries(ctx, &oda.LabelTimeSeriesReq{
// 		TimeSeries:   fmt.Sprint(tsid),
// 		LabelType:    "met.no/kvkafka",
// 		LabelContent: label,
// 	})
// 	if err != nil {
// 		return label, err
// 	}
// 	return label, nil
// }

// how to modify the obstime (in kdvh) for certain paramid
func cacheParamOffsets() (map[ParamKey]period.Period, error) {
	offsets := make(map[ParamKey]period.Period)

	// TODO: where does product_offsets.csv come from?
	type CSVRows struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int64  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}
	csvfile, err := os.Open("product_offsets.csv")
	if err != nil {
		return nil, err
	}
	defer csvfile.Close()

	csvrows := []CSVRows{}
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		return nil, err
	}

	for _, r := range csvrows {
		var fromtimeOffset, timespan period.Period
		if r.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(r.FromtimeOffset)
			if err != nil {
				return nil, err
			}
		}
		if r.Timespan != "" {
			timespan, err = period.Parse(r.Timespan)
			if err != nil {
				return nil, err
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			return nil, err
		}

		key := ParamKey{ElemCode: r.ElemCode, TableName: r.TableName}
		offsets[key] = migrationOffset
	}
	return offsets, nil
}

/*
step 4: SWITCH
related to products, we do not care about these?
*/

/*
step 5: VALIDATE This should really already be ensured at the dump step!
*/

func setLogFile(tableName, procedure string) {
	fileName := fmt.Sprintf("%s_%s_log.txt", tableName, procedure)
	fh, err := os.Create(fileName)

	if err != nil {
		// This should panic??
		log.Println("Could not create log file -", err)
		return
	}

	log.SetOutput(fh)
}
