package utils

import (
	"fmt"
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

// Returns:
// - "",                                if both t.From and t.To are nil
// - "from_<timestamp>",                if t.From is not nil, and t.To is nil
// - "to_<timestamp>",                  if t.From is nil, and t.To is not nil
// - "from_<timestamp>_to_<timestamp>", if both t.From and t.To are not nil
func (t *TimeSpan) ToDirName() string {
	if t.From != nil && t.To != nil {
		from := "from_" + t.From.Format(time.DateOnly)
		to := "to_" + t.To.Format(time.DateOnly)
		return from + "_" + to
	} else if t.From != nil {
		return "from_" + t.From.Format(time.DateOnly)
	} else if t.To != nil {
		return "to_" + t.To.Format(time.DateOnly)
	} else {
		// Move to separate dir?
		return ""
	}
}
