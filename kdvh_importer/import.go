package main

import (
	"bufio"
	"context"
	"errors"
	"io"

	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/rickb777/period"
)

// ParamKey is used for lookup of parameter offsets
type ParamKey struct {
	ElemCode  string `json:"ElemCode"`
	TableName string `json:"TableName"`
}

// Query from elem_map_cfnames_param
// TODO: these should be pointers or pgx/sql nulltypes?
type Metadata struct {
	ElemCode  string
	TableName string
	TypeID    int32
	ParamID   int32
	Hlevel    int32
	Sensor    int32
	Fromtime  *time.Time
	// totime    *time.Time
}

type ImportArgs struct {
	BaseDir  string `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep      string `long:"sep" default:";"  description:"Separator character in the dumped files"`
	Tables   string `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	Stations string `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	Elements string `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	// TODO: probably we don't need these, just use strings.Contains
	// Tables      []string
	// Stations    []string
	// Elements    []string
	OffsetMap   map[ParamKey]period.Period
	MetadataMap map[ParamKey]Metadata
}

// Implement Commander interface. This method is automatically called by go-flags while parsing the cmd
func (self *ImportArgs) Execute(args []string) error {
	err := self.cacheParamOffsets()
	if err != nil {
		log.Fatalln("Could not load param offsets:", err)
	}

	err = self.cacheMetadata()
	if err != nil {
		log.Fatalln("Could not load metadata from stinfosys:", err)
	}

	conn, err := pgx.Connect(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database: ", err)
	}
	defer conn.Close(context.TODO())

	for name, table := range TABLE2INSTRUCTIONS {
		if self.Tables != "" && !strings.Contains(self.Tables, name) {
			continue
		}
		// TODO: should be safe to spawn goroutines/waitgroup here with connection pool
		importTable(conn, table, self)
	}

	return nil
}

// Save metadata for later use by quering Stinfosys
func (self *ImportArgs) cacheMetadata() error {
	cache := make(map[ParamKey]Metadata)

	log.Println("Connecting to Stinfosys to cache metadata")
	conn, err := pgx.Connect(context.TODO(), os.Getenv("STINFO_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database: ", err)
	}
	defer conn.Close(context.TODO())

	elementList := strings.Split(self.Elements, ",")
	for name, ti := range TABLE2INSTRUCTIONS {
		if self.Tables != "" && !strings.Contains(self.Tables, name) {
			continue
		}

		query := "SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime " +
			"FROM elem_map_cfnames_param " +
			"WHERE table_name = $1 AND ($2::text[] IS NULL OR elem_code = ANY($2))"

		rows, err := conn.Query(context.TODO(), query, &ti.TableName, &elementList)
		if err != nil {
			return err
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Metadata])
		if err != nil {
			return err
		}

		for _, meta := range metas {
			cache[ParamKey{meta.TableName, meta.ElemCode}] = meta
		}
	}

	log.Println("Finished caching metadata")
	self.MetadataMap = cache
	return nil
}

// how to modify the obstime (in kdvh) for certain paramid
func (self *ImportArgs) cacheParamOffsets() error {
	offsets := make(map[ParamKey]period.Period)

	// TODO: which table does product_offsets.csv come from?
	type CSVRows struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int64  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}
	csvfile, err := os.Open("product_offsets.csv")
	if err != nil {
		return err
	}
	defer csvfile.Close()

	csvrows := []CSVRows{}
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		return err
	}

	for _, r := range csvrows {
		var fromtimeOffset, timespan period.Period
		if r.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(r.FromtimeOffset)
			if err != nil {
				return err
			}
		}
		if r.Timespan != "" {
			timespan, err = period.Parse(r.Timespan)
			if err != nil {
				return err
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			return err
		}

		key := ParamKey{ElemCode: r.ElemCode, TableName: r.TableName}
		offsets[key] = migrationOffset
	}

	self.OffsetMap = offsets
	return nil
}

// TODO: define the schema we want to export here
// And probably we should use nullable types?
type ObsLARD struct {
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
// TODO: maybe embed Timeseries?
type ObsKDVH struct {
	ID       int32
	elemCode string
	obsTime  time.Time
	data     string
	flags    string
	offset   period.Period
}

type Timeseries struct {
	ID     int32
	offset period.Period
	Metadata
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

func importTable(conn *pgx.Conn, table *TableInstructions, config *ImportArgs) {
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
	log.Println("Finished import of", table.TableName)
}

func getStationNumber(station os.DirEntry, stationList string) (int64, error) {
	if !station.IsDir() {
		return 0, errors.New(fmt.Sprintf("%s is not a directory, skipping", station.Name()))
	}

	if stationList != "" && !strings.Contains(stationList, station.Name()) {
		return 0, errors.New(fmt.Sprintf("Station %v not in the list, skipping", station.Name()))
	}

	stnr, err := strconv.ParseInt(station.Name(), 10, 64)
	if err != nil {
		return 0, errors.New("Error parsing station number:" + err.Error())
	}

	return stnr, nil
}

func getElementCode(element os.DirEntry, elementList string) (string, error) {
	elemCode := strings.TrimSuffix(element.Name(), ".csv")

	// skip if element is not in the given list
	if elementList != "" && !strings.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element %v not in the list, skipping", elemCode))
	}

	// check that this elemCode is valid and desireable (whatever that means)
	if isNotValid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element %v not set for import, skipping", elemCode))
	}

	return elemCode, nil
}

func parseData(handle io.ReadCloser, separator string, ts *Timeseries, table *TableInstructions) ([]ObsLARD, error) {
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
	data := make([]ObsLARD, 0)

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
			ObsKDVH{
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

func insertData(conn *pgx.Conn, data []ObsLARD) (int64, error) {
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

func getTimeseries(key ParamKey, stnr int64, conn *pgx.Conn, config *ImportArgs) (*Timeseries, error) {
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
