package db

import "testing"

func TestShouldProcessLabel(t *testing.T) {
	type TestCase struct {
		tag      string
		label    Label
		config   BaseConfig
		expected bool
	}

	cases := []TestCase{
		{
			tag:      "empty config",
			label:    Label{ParamID: 212},
			config:   BaseConfig{},
			expected: true,
		},
		{
			tag:      "label paramid in config paramids",
			label:    Label{ParamID: 212},
			config:   BaseConfig{ParamIds: []int32{212}},
			expected: true,
		},
		{
			tag:      "label paramid NOT in config paramids",
			label:    Label{ParamID: 212},
			config:   BaseConfig{ParamIds: []int32{300}},
			expected: false,
		},
		{
			tag:      "label level NOT in config level",
			label:    Label{},                        // nil level, but
			config:   BaseConfig{Levels: []int32{2}}, // required level == 2
			expected: false,
		},
		{
			tag: "label level in config levels",
			label: func() Label {
				var level int32 = 2
				return Label{Level: &level}
			}(),
			config:   BaseConfig{Levels: []int32{2}},
			expected: true,
		},
		{
			tag:      "Skipped paramID",
			label:    Label{ParamID: 200},
			config:   BaseConfig{SkipParamIds: []int32{200, 300}},
			expected: false,
		},
		{
			tag:      "paramID selected and skipped",
			label:    Label{ParamID: 200, TypeID: 500},
			config:   BaseConfig{ParamIds: []int32{200, 300}, SkipTypeIds: []int32{100, 500}},
			expected: false,
		},
		{
			tag:      "paramID selected, but typeID skipped",
			label:    Label{ParamID: 200, TypeID: 500},
			config:   BaseConfig{ParamIds: []int32{200, 300}, SkipTypeIds: []int32{100, 500}},
			expected: false,
		},
		{
			tag:      "label level NOT in config skiplevel",
			label:    Label{},
			config:   BaseConfig{SkipLevels: []int32{2}},
			expected: true,
		},
	}

	for _, c := range cases {
		res := c.config.ShouldProcessLabel(&c.label)
		if res != c.expected {
			t.Log("FAILED:", c.tag)
			t.Fail()
		} else {
			t.Log("PASSED:", c.tag)
		}
	}
}
