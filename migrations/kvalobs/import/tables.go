package port

import (
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
)

type ImportFunc func(file *os.File, tsid int64, label *kvalobs.Label, logStr string, pool *pgxpool.Pool) (int64, error)
type Table struct {
	Name     string
	ImportFn ImportFunc // Function that parses dumps and ingests observations into LARD
}

type Database struct {
	Name       string
	Tables     map[string]*Table
	ConnEnvVar string
}

func InitImportDBs() map[string]*Database {
	tables := map[string]*Table{
		kvalobs.DataTableName: {Name: kvalobs.DataTableName, ImportFn: importData},
		kvalobs.TextTableName: {Name: kvalobs.TextTableName, ImportFn: importText},
	}

	return map[string]*Database{
		kvalobs.KvDbName:   {Name: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar, Tables: tables},
		kvalobs.HistDbName: {Name: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar, Tables: tables},
	}
}
