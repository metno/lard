package port

import (
	kdvh "migrate/kdvh/db"
	"testing"
)

func TestFlagsAreValid(t *testing.T) {
	type testCase struct {
		input    kdvh.Obs
		expected bool
	}

	cases := []testCase{
		{kdvh.Obs{Flags: "12309"}, true},
		{kdvh.Obs{Flags: "984.3"}, false},
		{kdvh.Obs{Flags: ".1111"}, false},
		{kdvh.Obs{Flags: "1234."}, false},
		{kdvh.Obs{Flags: "12.2.4"}, false},
		{kdvh.Obs{Flags: "12.343"}, false},
		{kdvh.Obs{Flags: ""}, false},
		{kdvh.Obs{Flags: "asdas"}, false},
		{kdvh.Obs{Flags: "12a3a"}, false},
		{kdvh.Obs{Flags: "1sdfl"}, false},
	}

	for _, c := range cases {
		t.Log("Testing flag:", c.input.Flags)

		if result := flagsAreValid(&c.input); result != c.expected {
			t.Errorf("Got %v, wanted %v", result, c.expected)
		}
	}
}
