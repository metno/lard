package lard

import "strconv"

type qcRule struct {
	// Pattern matching a set of Kvalobs flags (useflag[1], useflag[2], useflag[3])
	pattern [3]int
	// Resulting code for a matched combination:
	//  - 0 -> OK, value is controlled and found OK.
	//  - 1 -> OK, value is controlled and corrected, or value is missing and interpolated.
	//  - 2 -> Slightly uncertain, value is not controlled.
	//  - 4 -> Slightly uncertain, value is not corrected.
	//  - 5 -> Very uncertain, value is not corrected.
	//  - 6 -> Very uncertain, model data. Value is controlled and corrected, or value is missing and
	//         automatically interpolated.
	//  - 7 -> Erroneous, value is not corrected.
	code int32
}

// Rules used in Frost (no default filtering in v0, defaults to [0, 1, 2, 4, 5] in frost-beta)
var QC_RULES []qcRule

// TODO: why are these rules non exhaustive?
func initRules() {
	QC_RULES = []qcRule{
		{pattern: [3]int{1, 9, 9}, code: 5},
		{pattern: [3]int{4, 9, 9}, code: 5},
		{pattern: [3]int{5, 9, 9}, code: 5},
		{pattern: [3]int{8, 9, 9}, code: 5},
		{pattern: [3]int{8, 9, 8}, code: 7},
		{pattern: [3]int{3, 3, 9}, code: 7},
		{pattern: [3]int{3, 0, 8}, code: 7},
		{pattern: [3]int{0, 0, 8}, code: 7}, // How can this even exist??

		{pattern: [3]int{-1, -1, 1}, code: 1},
		{pattern: [3]int{-1, -1, 2}, code: 1},
		{pattern: [3]int{-1, -1, 3}, code: 6}, // Why are (5,6) trusted, while (3,4) are not?
		{pattern: [3]int{-1, -1, 4}, code: 6},
		{pattern: [3]int{-1, -1, 5}, code: 1},
		{pattern: [3]int{-1, -1, 6}, code: 1},

		{pattern: [3]int{-1, 0, -1}, code: 0},
		{pattern: [3]int{-1, 9, -1}, code: 2},

		{pattern: [3]int{-1, 1, 0}, code: 4},
		{pattern: [3]int{-1, 2, 0}, code: 5},
		{pattern: [3]int{-1, 3, 0}, code: 7},
		{pattern: [3]int{-1, 3, 8}, code: 7}, // How can this even exist??
	}
}

// Extracts a subset of the `useinfo` flag used to match against the QC rule patterns.
// `useinfo` is expected to be at least 5 numeric char long
func extractFlag(useinfo string) [3]int {
	first, _ := strconv.Atoi(useinfo[1:2])
	secon, _ := strconv.Atoi(useinfo[2:3])
	third, _ := strconv.Atoi(useinfo[3:4])
	return [3]int{first, secon, third}
}

// Checks if the flag extracted from the given useinfo matches
// any of the QC rules, and returns the corresponding quality code
func GetQualityCode(useinfo string) *int32 {
	if QC_RULES == nil {
		initRules()
	}

	flag := extractFlag(useinfo)
outer:
	for _, rule := range QC_RULES {
		for i := range 3 {
			if !(rule.pattern[i] < 0 || rule.pattern[i] == flag[i]) {
				continue outer
			}
		}
		return &rule.code
	}

	return nil
}
