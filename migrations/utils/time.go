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
func (t *TimeSpan) ToString() string {
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

// Returns a Timespan parsed from string in the form of "(from_<timestamp>)(_)(to_<timestamp>)"
// func SpanFromPath(path string) (*TimeSpan, error) {
// 	split := strings.Split(path, "_")
// 	switch len(split) {
// 	case 4:
// 		// from, from_timespan, to, to_timespan
// 		from, err := time.Parse(time.DateOnly, split[1])
// 		if err != nil {
// 			return nil, fmt.Errorf("Could not parse '%s'", path)
// 		}
// 		to, err := time.Parse(time.DateOnly, split[3])
// 		if err != nil {
// 			return nil, fmt.Errorf("Could not parse '%s'", path)
// 		}
// 		return &TimeSpan{From: &from, To: &to}, nil
// 	case 2:
// 		switch split[0] {
// 		case "from":
// 			// from, from_timespan
// 			from, err := time.Parse(time.DateOnly, split[1])
// 			if err != nil {
// 				return nil, fmt.Errorf("Could not parse '%s'", path)
// 			}
// 			return &TimeSpan{From: &from}, nil
// 		case "to":
// 			// to, to_timespan
// 			to, err := time.Parse(time.DateOnly, split[1])
// 			if err != nil {
// 				return nil, fmt.Errorf("Could not parse '%s'", path)
// 			}
// 			return &TimeSpan{To: &to}, nil
// 		}
// 		fallthrough
// 	default:
// 		return nil, fmt.Errorf("Could not parse '%s'", path)
// 	}
// }
