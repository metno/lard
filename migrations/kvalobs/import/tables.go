package port

import (
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
)

type ImportFunc func(file *os.File, tsid int64, label *kvalobs.Label, logStr string, pool *pgxpool.Pool) (int64, error)
type Table struct {
	Name       string
	DbName     string
	ConnEnvVar string
	ImportFn   ImportFunc // Function that parses dumps and ingests observations into LARD
}

type Database struct {
	DBName     string
	Tables     map[string]*Table
	ConnEnvVar string
}

func InitImportTables() []*Table {
	return []*Table{
		{Name: kvalobs.DataTableName, DbName: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar, ImportFn: importData},
		{Name: kvalobs.DataTableName, DbName: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar, ImportFn: importData},
		{Name: kvalobs.TextTableName, DbName: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar, ImportFn: importText},
		{Name: kvalobs.TextTableName, DbName: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar, ImportFn: importText},
	}
}
