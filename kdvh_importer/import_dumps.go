package main

import (
	"bufio"
	"context"
	"errors"
	"io"

	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rickb777/period"
)

// TODO: define the schema we want to export here
// And probably we should use nullable types?
type Observation struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Observation data formatted as a double precision floating point
	Data float64
	// Observation data formatted as a binary large object.
	// TODO: what is this?
	DataBlob []byte
	// Estimated observation data from KDVH (unknown method)
	// NOTE: unknown method ??????????
	CorrKDVH float64
	// Latest updated corrected observation data value
	// Corrected float64
	// Flag encoding quality control status
	KVFlagControlInfo []byte
	// Flag encoding quality control status
	KVFlagUseInfo []byte
	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
	// KDVHFlag []byte
	// Comma separated value listing checks that failed during quality control
	KVCheckFailed string
}

// Struct holding all the info needed from KDVH
type KDVHData struct {
	ID       int32
	elemCode string
	obsTime  time.Time
	data     string
	flags    string
	offset   period.Period
}

// DataPageFunction is a function that creates a properly formatted TimeSeriesData object
type DataPageFunction func(KDVHData) (Observation, error)

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
// type KDVHKey struct {
// 	Stnr int64 `json:"Stnr"`
// 	ParamKey
// }

// TODO: this would be queried directly from KDVH?
// better than querying stinfosys?
// type TimeSeriesInfoKDVH struct {
// 	Stnr     int64      `db:"stnr"`
// 	ElemCode string     `db:"elem_code"`
// 	FromTime *time.Time `db:"fdato"`
// 	ToTime   *time.Time `db:"tdato"`
// }

// define how to treat different tables
var TABLE2INSTRUCTIONS = map[string]*TableInstructions{
	// unique tables imported in their entirety
	"T_EDATA":                {TableName: "T_EDATA", FlagTableName: "T_EFLAG", ElemTableName: "T_ELEM_EDATA", CustomDataFunction: makeDataPageEdata, ImportUntil: 3001},
	"T_METARDATA":            {TableName: "T_METARDATA", ElemTableName: "T_ELEM_METARDATA", CustomDataFunction: makeDataPage, ImportUntil: 3000},
	"T_DIURNAL_INTERPOLATED": {TableName: "T_DIURNAL_INTERPOLATED", CustomDataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	"T_MONTH_INTERPOLATED":   {TableName: "T_MONTH_INTERPOLATED", CustomDataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	// tables with some data in kvalobs, import only up to 2005-12-31
	"T_ADATA":      {TableName: "T_ADATA", FlagTableName: "T_AFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 2006},
	"T_MDATA":      {TableName: "T_MDATA", FlagTableName: "T_MFLAG", ElemTableName: "T_ELEM_OBS", CustomDataFunction: makeDataPage, ImportUntil: 3000},
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

func importTable(table *TableInstructions, config *MigrationConfig) {
	// send an email if something panics inside here
	// defer EmailOnPanic("importTable")

	if table.ImportUntil == 0 {
		log.Printf("Skipping import of %s because this table is not set for import\n", table.TableName)
		return
	}

	log.Println("Starting import of", table.TableName)
	setLogFile(table.TableName, "import")

	// Get station dirs from table directory
	path := fmt.Sprintf("%s%s_combined", config.BaseDir, table.TableName)
	stations, err := os.ReadDir(path)
	if err != nil {
		log.Println("importTable os.ReadDir on", path, "-", err)
		return
	}

	// Connect to LARD database
	conn, err := pgx.Connect(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database: ", err)
	}

	// loop over stations found in the directory
	for _, station := range stations {
		stnr, err := getStationNumber(station, config.Stations)
		if err != nil {
			log.Println("getStationNumber -", err)
			continue
		}

		stationDir := fmt.Sprintf("%v/%v", path, stnr)
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			log.Println("importTable - os.ReadDir on", stationDir, "-", err)
			continue
		}

		// loop over element files in each station directory
		for _, element := range elements {
			elemCode, err := getElementCode(element, config.Elements)
			if err != nil {
				log.Println("getElementCode -", err)
				continue
			}

			filename := fmt.Sprintf("%s/%s", stationDir, element.Name())
			handle, err := os.Open(filename)
			if err != nil {
				// SendEmail(
				// 	"ODA - KDVH importer worker crashed",
				// 	"In importTimeSeriesWorker os.Open:\n"+err.Error(),
				// )
				log.Fatalln("importStationData os.Open -", err)
			}

			// get Timeseries information
			key := ParamKey{table.TableName, elemCode}
			timeseries, err := getTimeseries(key, stnr, conn, config)
			if err != nil {
				log.Println("Error on obtaining timeseries info:", err)
				continue
			}

			data, err := parseData(handle, config.Sep, timeseries, table)
			if err != nil {
				log.Println("parseData -", err)
				continue
			}

			count, err := insertData(conn, data)
			if err != nil {
				log.Println("Failed bulk insertion", err)
				continue
			}

			log.Printf("importTable - %v, station %v, element %v: inserted %v rows\n",
				table.TableName, stnr, elemCode, count,
			)
			if int(count) != len(data) {
				log.Printf("WARN: Not all rows have been inserted (%v/%v)\n", count, len(data))
			}
		}

	}

	log.SetOutput(os.Stdout)
	if table.FlagTableName != "" {
		log.Println("Finished data and flag import of", table.TableName)
	} else {
		log.Println("Finished data import of", table.TableName)
	}
}

func getStationNumber(station os.DirEntry, stationList []string) (int64, error) {
	if !station.IsDir() {
		return 0, errors.New(fmt.Sprintf("%s is not a directory, skipping", station.Name()))
	}

	if stationList != nil && !slices.Contains(stationList, station.Name()) {
		return 0, errors.New(fmt.Sprintf("Station %v not in the list, skipping", station.Name()))
	}

	stnr, err := strconv.ParseInt(station.Name(), 10, 64)
	if err != nil {
		return 0, errors.New("Error parsing station number:" + err.Error())
	}

	return stnr, nil
}

func getElementCode(element os.DirEntry, elementList []string) (string, error) {
	elemCode := strings.TrimSuffix(element.Name(), ".csv")

	// skip if element is not in the given list
	if elementList != nil && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element %v not in the list, skipping", elemCode))
	}

	// check that this elemCode is valid and desireable (whatever that means)
	if isNotValid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element %v not set for import, skipping", elemCode))
	}

	return elemCode, nil
}

func parseData(handle io.ReadCloser, separator string, ts *Timeseries, table *TableInstructions) ([]Observation, error) {
	scanner := bufio.NewScanner(handle)

	// check header row: maybe it's semicolon, maybe it's comma! :(
	cols := make([]string, 2)
	scanner.Scan()

	cols = strings.Split(scanner.Text(), separator)
	if len(cols) < 2 {
		err := handle.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("First line of file was:" + scanner.Text() + "- was the separator wrong?")
	}

	// use relevant fromtime and totime from ElemTableName to avoid importing nonsense flags
	// TODO: what does this mean?
	// kdvhinfo, ok := TableTimeSeriesKDVH[fi.KDVHKeys]
	// if !ok {
	// that is okay though, orig_obstime is not defined in elem tables
	// TODO: again what does this mean?
	// log.Println("TimeSeries not recognized in KDVH cache", fi.KDVHKeys)
	// }
	data := make([]Observation, 0)

	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), separator)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			log.Println("Error parsing time:", err)
			continue
		}

		// TODO: so we actually need to either query KDVH directly or the proxy?
		// only import data between kdvh's defined fromtime and totime
		// if kdvhinfo != nil { if kdvhinfo.FromTime != nil && obsTime.Sub(*kdvhinfo.FromTime) < 0 { continue }
		// if kdvhinfo.ToTime != nil && obsTime.Sub(*kdvhinfo.ToTime) > 0 { break } }

		if obsTime.Year() >= int(table.ImportUntil) {
			log.Println("Reached max import time for this table, inserting data")
			break
		}

		tempdata, err := table.CustomDataFunction(
			KDVHData{
				ID:       ts.ID,
				elemCode: ts.ElemCode,
				offset:   ts.offset,
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

	return data, nil
}

func insertData(conn *pgx.Conn, data []Observation) (int64, error) {
	// TODO: should this be a transaction?
	return conn.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "obsvalue", "controlinfo", "useinfo"},
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

type Timeseries struct {
	ID     int32
	offset period.Period
	Metadata
}

func getTimeseries(key ParamKey, stnr int64, conn *pgx.Conn, config *MigrationConfig) (*Timeseries, error) {
	var tsid int32

	// parameter offset
	offset := config.OffsetMap[key]

	// metadata
	meta, ok := config.MetadataMap[key]
	if !ok {
		// TODO: missing entry in Stinfo for that KDVHkey
		log.Println("Missing entry in Stinfosys")
		return nil, errors.New("Missing metadata in Stinfosys")
	}

	// Query LARD labels table with stinfosys metadata
	err := conn.QueryRow(context.TODO(), `SELECT timeseries FROM labels.met
      WHERE station_id = $1
      AND param_id = $2
      AND type_id = $3
      AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
      AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		&stnr, &meta.ParamID, &meta.TypeID, &meta.Hlevel, &meta.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID
	if err == nil {
		return &Timeseries{tsid, offset, meta}, nil
	}

	// Otherwise insert new timeseries
	transaction, err := conn.Begin(context.TODO())
	if err != nil {
		return nil, err
	}

	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id`,
		meta.Fromtime,
	).Scan(&tsid)
	if err != nil {
		return nil, err
	}

	_, err = transaction.Exec(context.TODO(), `INSERT INTO labels.met
      (timeseries, station_id, param_id, type_id, lvl, sensor)
      VALUES ($1, $2, $3, $4, $5, $6)`,
		&tsid, &stnr, &meta.ParamID, &meta.TypeID, &meta.Hlevel, &meta.Sensor)
	if err != nil {
		return nil, err
	}

	err = transaction.Commit(context.TODO())
	if err != nil {
		return nil, err
	}

	// TODO: I guess we don't care about this conversion?
	// relabel to kvkafka if wanted and if possible
	// if ti.ImportUntil > 0 {
	// 	kvkafka, err := relabelKdvh2Kvkafka(ti, keys, resp.GetID(), s)
	// 	if err != nil {
	// 		log.Println("Could not relabel", label, "to", kvkafka, "-", err)
	// 	}
	// }

	return &Timeseries{tsid, offset, meta}, nil
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
