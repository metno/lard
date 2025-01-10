package utils

import (
	"fmt"
	// "strings"
	"time"
)

type Timestamp struct {
	t time.Time
}

func (ts *Timestamp) UnmarshalText(b []byte) error {
	t, err := time.Parse(time.DateOnly, string(b))
	if err != nil {
		return fmt.Errorf("Only the date-only format (\"YYYY-MM-DD\") is allowed. Got %s", b)
	}
	ts.t = t
	return nil
}

func (ts *Timestamp) Inner() *time.Time {
	if ts == nil {
		return nil
	}
	return &ts.t
}

type TimeSpan struct {
	From *time.Time
	To   *time.Time
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
