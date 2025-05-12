package port

import (
	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
)

// How to convert a dumped CSV row to a LARD Observation
type ParseFunc func(tsid int64, row string) (*lard.ParsedObs, error)
type Table struct {
	Name       string
	DbName     string
	ConnEnvVar string
}

func (table *Table) getParser(label *kvalobs.Label) ParseFunc {
	if label.IsMetarCloudType() {
		return parseMetarCloudType
	}

	if label.IsSpecialCloudType() {
		return parseSpecialCloudType
	}

	switch table.Name {
	case kvalobs.DataTableName:
		return parseData
	case kvalobs.TextTableName:
		return parseText
	}

	return nil
}

func InitImportTables() []*Table {
	return []*Table{
		{Name: kvalobs.DataTableName, DbName: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar},
		{Name: kvalobs.DataTableName, DbName: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar},
		{Name: kvalobs.TextTableName, DbName: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar},
		{Name: kvalobs.TextTableName, DbName: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar},
	}
}
