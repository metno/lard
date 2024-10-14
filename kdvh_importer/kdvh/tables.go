package kdvh

import (
	"database/sql"
	"fmt"

	"kdvh_importer/dump"
)

type T_ADATA KDVHTable

func (t *T_ADATA) dump(conn *sql.Conn, config *dump.Config) {

}

func (t *T_ADATA) FetchStationsWithElement(element string) {
	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s WHERE %s IS NOT NULL`,
		t.TableName,
		element,
	)
}

type T_HOMOGEN_MONTH KDVHTable

// var T_HOMOGEN_MONTH HOMOGEN_MONTH = HOMOGEN_MONTH{TableName: "T_HOMOGEN_MONTH"}

func (t *T_HOMOGEN_MONTH) getElements() ([]string, error) {
	return []string{"rr", "tam"}, nil
}

func (t *T_HOMOGEN_MONTH) dump(element, station string, conn *sql.DB) (*sql.Rows, error) {
	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		element,
	)

	return conn.Query(query, station)

	// rows, err := conn.Query(query, station)
	// if err != nil {
	// 	slog.Error(err.Error())
	// 	return err
	// }

	// TODO: should be moved outside
	// if err := dumpToFile(args.path, args.element, rows); err != nil {
	// 	slog.Error(err.Error())
	// 	return err
	// }

	// return nil
}

type T_MINUTE KDVHTable

func (t *T_MINUTE) dump(element, station string, conn *sql.DB) (*sql.Rows, error)

func dumpzz(args dumpFuncArgs, conn *sql.DB) error {
	dataBegin, dataEnd, err := fetchYearRange(args.dataTable, args.station, conn)
	if err != nil {
		return err
	}

	flagBegin, flagEnd, err := fetchYearRange(args.flagTable, args.station, conn)
	if err != nil {
		return err
	}

	begin := min(dataBegin, flagBegin)
	end := max(dataEnd, flagEnd)

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
		args.element,
		args.dataTable,
		args.flagTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, args.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			return err
		}

		path := filepath.Join(args.path, string(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := dumpToFile(path, args.element, rows); err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return nil
}
