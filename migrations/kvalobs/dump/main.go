package dump

import (
	"fmt"
	"migrate/kvalobs/db"
	"migrate/utils"

	"github.com/joho/godotenv"
)

// TODO: there were some comments in the original script about
// the fact that the same timeseries could be in both
// 'data' and 'text_data'

type Config struct {
	db.BaseConfig
	From         *utils.Timestamp `arg:"-f" help:"Fetch data only starting from this date-only timestamp. Required if --to is not provided."`
	To           *utils.Timestamp `arg:"-t" help:"Fetch data only until this date-only timestamp. Required if --from is not provided."`
	LabelFile    string           `arg:"-l" help:"Specify a file to use instead of fetching the labels. Works only if 'db' is set."`
	LabelsOnly   bool             `arg:"--labels-only" help:"Only dump labels"`
	UpdateLabels bool             `arg:"--labels-update" help:"Overwrites the label CSV files"`
	MaxConn      int              `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to Kvalobs"`
	Overwrite    bool             `help:"Overwrite dumped files that match the span directory"`
	Timespan     *utils.TimeSpan  `arg:"-"`
}

func (Config) Description() string {
	return `Dump tables from Kvalobs.
The following environement variables need to be set:
	- "KVALOBS_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) SetTimespan() error {
	from := config.From.Inner()
	to := config.To.Inner()
	if from == nil && to == nil {
		return fmt.Errorf("It is required to provide the --from or --to flags.")
	}

	config.Timespan = &utils.TimeSpan{From: from, To: to}
	return nil
}

func (config *Config) Execute() {
	if err := config.SetTimespan(); err != nil {
		fmt.Println(err)
		return
	}

	if config.LabelFile != "" && config.Database == "" {
		fmt.Println("The '-l' flag works only if the database is specified.")
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	dbs := db.InitDBs()
	for name, db := range dbs {
		if !utils.StringIsEmptyOrEqual(config.Database, name) {
			continue
		}
		dumpDB(db, config)
	}
}
