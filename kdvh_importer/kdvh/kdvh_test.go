package kdvh

import "testing"

func TestIsReal(t *testing.T) {
	type testCase struct {
		input    string
		expected bool
	}

	cases := []testCase{
		{"12309", true},
		{"12.343", true},
		{"984.3", true},
		{"12.2.4", false},
		{"1234.", true},
		{"", false},
		{"asdas", false},
		{"12a3a", false},
		{"1sdfl", false},
	}

	for _, c := range cases {
		t.Log("Testing flag:", c.input)

		if result := IsReal(c.input); result != c.expected {
			t.Errorf("Got %v, wanted %v", result, c.expected)
		}
	}
}
