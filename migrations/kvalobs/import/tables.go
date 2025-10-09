package port

import (
	kvalobs "migrate/kvalobs/db"
)

type Table struct {
	Name string
}

func NewTable(name string) *Table {
	return &Table{name}
}

func InitImportTables() []*Table {
	return []*Table{
		{Name: kvalobs.DataTableName},
		{Name: kvalobs.TextTableName},
	}
}
