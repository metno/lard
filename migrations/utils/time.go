package utils

import (
	"fmt"
	"strings"
	"time"
)

// type Timestamp time.Time
type Timestamp struct {
	t time.Time
}

func (ts *Timestamp) UnmarshalText(b []byte) error {
	// Hack for empty `--to` flag
	// `--from` defaults to '1700-01-01'
	if string(b) == "now" {
		now, err := time.Parse(time.DateOnly, time.Now().Format(time.DateOnly))
		if err != nil {
			fmt.Println(err)
			return err
		}
		ts.t = now
		return nil
	}

	t, err := time.Parse(time.DateOnly, string(b))
	if err != nil {
		return fmt.Errorf("Only the date-only format (\"YYYY-MM-DD\") is allowed. Got %s", b)
	}
	ts.t = t
	return nil
}

func (ts *Timestamp) After(other Timestamp) bool {
	return ts.t.After(other.t)
}

type TimeSpan struct {
	From *time.Time
	To   *time.Time
}

func NewTimespan(from, to Timestamp) TimeSpan {
	f := time.Time(from.t)
	t := time.Time(to.t)
	return TimeSpan{
		From: &f,
		To:   &t,
	}
}

func (t *TimeSpan) ToDirName() (string, error) {
	if t.From == nil || t.To == nil {
		return "", fmt.Errorf("Can only convert timespan with non-nil fields to dirname")
	}
	dirname := fmt.Sprintf(
		"from_%s_to_%s",
		t.From.Format(time.DateOnly),
		t.To.Format(time.DateOnly),
	)
	return dirname, nil
}

// Deserializes name of the directory to a timespan.
// `name` format is expected to be: 'from_<from_date>_to_<to_date>'
func TimespanFromDirName(name string) (*TimeSpan, error) {
	// fields = {'from', '<from_date>', 'to', '<to_date>'}
	fields := strings.Split(name, "_")

	from, ferr := time.Parse(time.DateOnly, fields[1])
	to, terr := time.Parse(time.DateOnly, fields[3])
	if ferr != nil || terr != nil {
		return nil, fmt.Errorf("Could not parse dirname: %s", name)
	}

	return &TimeSpan{&from, &to}, nil
}
