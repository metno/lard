package lard

import "testing"

func addr[T any](v T) *T {
	return &v
}

func TestFlagsAreValid(t *testing.T) {
	type testCase struct {
		useinfo string
		code    *int32
	}

	cases := []testCase{
		{useinfo: "00010", code: addr(int32(1))},
		{useinfo: "00020", code: addr(int32(1))},
		{useinfo: "00030", code: addr(int32(6))},
		{useinfo: "00040", code: addr(int32(6))},
		{useinfo: "00050", code: addr(int32(1))},
		{useinfo: "00060", code: addr(int32(1))},
		{useinfo: "08990", code: addr(int32(5))},
		{useinfo: "09990", code: addr(int32(7))},
		{useinfo: "08980", code: addr(int32(7))},
		{useinfo: "01190", code: nil},
	}

	for _, c := range cases {
		code, _ := GetQualityCode(c.useinfo)
		if code == c.code || ((code != nil && c.code != nil) && (*code == *c.code)) {
			t.Log("PASSED:", c.useinfo)
		} else {
			t.Log("FAILED:", c.useinfo, "got", code, "wanted", c.code)
			t.Fail()
		}
	}
}
