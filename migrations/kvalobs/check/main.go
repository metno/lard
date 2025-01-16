package check

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	kvalobs "migrate/kvalobs/db"
	"migrate/kvalobs/dump"
	"migrate/stinfosys"

	"github.com/joho/godotenv"
)

type Config struct {
	DataFilename string `arg:"positional" required:"true" help:"data label file"`
	TextFilename string `arg:"positional" required:"true" help:"text label file"`
}

func (Config) Description() string {
	return `Checks if there are inconsistencies between kvalobs and stinfosys.
Requires a set of dumped kvalobs label files and the "STINFO_CONN_STRING" environement variable.`
}

func (c *Config) Execute() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	dataParamids, derr := loadParamids(c.DataFilename)
	textParamids, terr := loadParamids(c.TextFilename)
	if derr != nil || terr != nil {
		fmt.Println(errors.Join(derr, terr))
		return
	}

	fmt.Println("Checking if some param IDs are stored in both the `data` and `text_data` tables")
	c.checkDataAndTextParamsOverlap(dataParamids, textParamids)

	fmt.Println("Checking if param IDs in `text_data` match non-scalar parameters in Stinfosys")
	conn, ctx := stinfosys.Connect()
	defer conn.Close(ctx)
	stinfoParams := stinfosys.GetNonScalars(conn)
	c.checkNonScalars(dataParamids, textParamids, stinfoParams)
}

// Simply checks if some params are found both in the data and text_data
func (c *Config) checkDataAndTextParamsOverlap(dataParamids, textParamids map[int32]int32) {
	defer fmt.Println(strings.Repeat("- ", 40))

	ids := make([]int32, 0, len(textParamids))
	for id := range dataParamids {
		if _, ok := textParamids[id]; ok {
			ids = append(ids, id)
		}
	}

	slices.Sort(ids)
	for _, id := range ids {
		fmt.Printf("ParamID %5d exists in both data and text tables\n", id)
	}
}

func loadParamids(path string) (map[int32]int32, error) {
	labels, err := dump.ReadLabelCSV(path)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	paramids := uniqueParamids(labels)
	return paramids, nil

}

// Creates hashset of paramids
func uniqueParamids(labels []*kvalobs.Label) map[int32]int32 {
	paramids := make(map[int32]int32)
	for _, label := range labels {
		paramids[label.ParamID] += 1
	}
	return paramids
}

type StinfoPair struct {
	ParamID  int32 `db:"paramid"`
	IsScalar bool  `db:"scalar"`
}

// Checks that text params in Kvalobs are considered non-scalar in Stinfosys
func (c *Config) checkNonScalars(dataParamids, textParamids map[int32]int32, nonscalars []int32) {
	defer fmt.Println(strings.Repeat("- ", 40))

	for _, id := range nonscalars {
		if _, ok := textParamids[id]; ok {
			fmt.Printf("MATCH: ParamID %5d is text in both Stinfosys and Kvalobs\n", id)
			delete(textParamids, id)
		} else if _, ok := dataParamids[id]; ok {
			fmt.Printf(" FAIL: ParamID %5d is text in Stinfosys, but not in Kvalobs\n", id)
		} else {
			fmt.Printf(" WARN: ParamID %5d not found in Kvalobs\n", id)
		}
	}

	idsLeft := make([]int32, 0, len(textParamids))
	for id := range textParamids {
		idsLeft = append(idsLeft, id)
	}

	slices.Sort(idsLeft)
	for _, id := range idsLeft {
		fmt.Printf(" FAIL: ParamID %5d is text in Kvalobs, but not in Stinfosys\n", id)
	}

}
