package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"

	kdvh "migrate/kdvh/db"
	"migrate/utils"
)

type Config struct {
	kdvh.BaseConfig
	OverwriteData bool            `help:"Overwrite existing dumped data files"`
	OverwriteTxt  bool            `help:"Overwrite existing element.txt and station.txt files"`
	MaxConn       int             `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to KDVH"`
	From          utils.Timestamp `default:"1700-01-01" help:"Fetch data only starting from this date-only timestamp."`
	To            utils.Timestamp `default:"now" help:"Fetch data only until this date-only timestamp. Defaults to today's date if not set."`
	Timespan      utils.TimeSpan  `arg:"-"`
}

func (Config) Description() string {
	return `Dump tables from KDVH.
The \"KDVH_PROXY_CONN_STRING\" environement variable is required for this command`
}

func (config *Config) SetTimespan() error {
	if config.From.After(config.To) {
		return fmt.Errorf("Error: --from can't be after --to")
	}
	config.Timespan = utils.NewTimespan(config.From, config.To)
	return nil
}

func (config *Config) Execute() {
	if err := config.SetTimespan(); err != nil {
		fmt.Println(err)
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv(kdvh.KDVH_ENV_VAR))
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	spanPath, err := config.Timespan.ToDirName()
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	tables := InitDumpTables()
	for _, table := range tables {
		if !config.ShouldProcessTable(table.TableName) {
			continue
		}

		// TODO: need to mkdir if we want to pass config.Path here
		handle := utils.SetLoggerOutput(table.TableName, "dump")
		defer handle.Close()

		path := filepath.Join(config.Path, spanPath, table.TableName)

		table.Dump(path, pool, config)
	}
}
