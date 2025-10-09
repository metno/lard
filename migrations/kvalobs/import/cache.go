package port

import (
	"migrate/stinfosys"
)

type Cache struct {
	Permits stinfosys.PermitMaps
	Levels  stinfosys.ParamLevelMap
}

func NewCache() *Cache {
	conn, ctx := stinfosys.Connect()
	defer conn.Close(ctx)

	permits := stinfosys.NewPermitTables(conn)
	levels := stinfosys.CacheParamLevels(conn)

	return &Cache{Permits: permits, Levels: levels}
}

func (c *Cache) GetPermit(stnr, typeid, paramid int32) *int32 {
	return c.Permits.GetPermit(stnr, typeid, paramid)
}
