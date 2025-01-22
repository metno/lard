package dump

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

// TODO: there were some comments in the original script about
// the fact that the same timeseries could be in both
// 'data' and 'text_data'

type Config struct {
	kvalobs.BaseConfig
	From         utils.Timestamp `arg:"-f" default:"1700-01-01" help:"Fetch data only starting from this date-only timestamp."`
	To           utils.Timestamp `arg:"-t" default:"now" help:"Fetch data only until this date-only timestamp. Defaults to today's date if not set."`
	LabelFile    string          `arg:"-l" help:"File to use instead of fetching the labels. Makes sense only if 'db' and 'table' are set."`
	LabelsOnly   bool            `arg:"--labels-only" help:"Only dump labels"`
	UpdateLabels bool            `arg:"--labels-update" help:"Overwrites the label CSV files"`
	MaxConn      int             `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to Kvalobs"`
	Overwrite    bool            `help:"Overwrite dumped files that match the span directory"`
	Timespan     utils.TimeSpan  `arg:"-"`
}

func (Config) Description() string {
	return `Dump tables from Kvalobs.
The following environement variables need to be set:
	- "KVALOBS_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) SetTimespan() error {
	if config.From.After(config.To) {
		return fmt.Errorf("Error: --from can't be after --to")
	}
	config.Timespan = utils.NewTimespan(config.From, config.To)
	return nil
}

func (config *Config) checkLabelFile() error {
	if config.LabelFile != "" {
		if config.Database == "" && config.Table == "" {
			return fmt.Errorf("The '-l' flag only works if the '--db' and '--table' are also specified.")
		}
		if config.LabelsOnly || config.UpdateLabels {
			return fmt.Errorf("The '-l' flag is not compatible with '--labels-only' nor '--labels-update'")
		}
	}
	return nil
}

func (config *Config) LoadLabels() ([]*kvalobs.Label, error) {
	labels, err := ReadLabelCSV(config.LabelFile)
	if err != nil {
		return nil, err
	}
	return labels, nil
}

func (config *Config) Execute() {
	if err := config.SetTimespan(); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(config)
	fmt.Println(config.From, config.To, config.Timespan)
	os.Exit(0)

	if err := config.checkLabelFile(); err != nil {
		fmt.Println(err)
		return
	}

	if err := config.CheckSpelling(); err != nil {
		fmt.Println(err)
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	dbs := initDumpDBs()
	for name, db := range dbs {
		if !utils.StringIsEmptyOrEqual(config.Database, name) {
			continue
		}
		db.dump(config)
	}
}
