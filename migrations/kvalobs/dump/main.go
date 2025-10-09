package dump

import (
	"fmt"

	"github.com/joho/godotenv"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

type Config struct {
	kvalobs.BaseConfig
	Database  string          `arg:"--db" help:"Which database to process, all by default. Choices: ['kvalobs', 'histkvalobs']"`
	From      utils.Timestamp `arg:"-f" default:"1700-01-01" help:"Fetch data only starting from this date-only timestamp."`
	To        utils.Timestamp `arg:"-t" default:"now" help:"Fetch data only until this date-only timestamp. Defaults to today's date if not set."`
	MaxConn   int             `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to Kvalobs"`
	Overwrite bool            `help:"Overwrite dumped files that match the span directory"`
	Timespan  utils.TimeSpan  `arg:"-"`
}

func (Config) Description() string {
	return `Dump tables from Kvalobs.
The following environement variables need to be set:
	- "KVALOBS_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) CheckDbSpelling() error {
	switch config.Database {
	case "", kvalobs.KvDbName, kvalobs.HistDbName:
	default:
		return fmt.Errorf("The '--db' flag expects either 'kvalobs' or 'histkvalobs' as input, got '%s'", config.Database)
	}

	return nil
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

	if err := config.CheckTableSpelling(); err != nil {
		fmt.Println(err)
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	spanPath, err := config.Timespan.ToDirName()
	if err != nil {
		fmt.Println(err)
		return
	}

	dbs := initDumpDBs()
	for _, db := range dbs {
		if !utils.StringIsEmptyOrEqual(config.Database, db.Name) {
			continue
		}

		db.dump(spanPath, config)
	}
}
