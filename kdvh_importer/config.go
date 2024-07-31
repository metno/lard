package main

import (
	"context"
	"log"
	"os"
	"slices"
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

type MigrationConfig struct {
	BaseDir     string
	Sep         string
	Tables      []string
	Stations    []string
	Elements    []string
	OffsetMap   map[ParamKey]period.Period
	MetadataMap map[ParamKey]Metadata
}

func NewMigrationConfig(args *CmdArgs) *MigrationConfig {
	var tables, stations, elements []string

	if args.TableList != "" {
		tables = strings.Split(args.TableList, ",")
	}
	if args.StationList != "" {
		stations = strings.Split(args.StationList, ",")
	}
	if args.ElemCodeList != "" {
		elements = strings.Split(args.ElemCodeList, ",")
	}

	OffsetMap, err := cacheParamOffsets()
	if err != nil {
		log.Fatalln("Could not load param offsets:", err)
	}

	return &MigrationConfig{
		BaseDir:   args.BaseDir,
		Sep:       args.Sep,
		Tables:    tables,
		Stations:  stations,
		Elements:  elements,
		OffsetMap: OffsetMap,
	}
}

func (self *MigrationConfig) cacheMetadata() error {
	cache := make(map[ParamKey]Metadata)

	log.Println("Connecting to Stinfosys to cache metadata")
	conn, err := pgx.Connect(context.TODO(), os.Getenv("STINFO_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to database: ", err)
	}

	for name, ti := range TABLE2INSTRUCTIONS {
		if self.Tables != nil && !slices.Contains(self.Tables, name) {
			continue
		}

		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime FROM elem_map_cfnames_param
                  WHERE table_name = $1 AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, &ti.TableName, &self.Elements)
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
func cacheParamOffsets() (map[ParamKey]period.Period, error) {
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
