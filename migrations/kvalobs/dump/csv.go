package dump

import (
	"fmt"
	"migrate/kvalobs/db"
	"os"

	"github.com/gocarina/gocsv"
)

func ReadLabelCSV(path string) (labels []*db.Label, err error) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer file.Close()

	fmt.Printf("Reading previously dumped labels from %s...\n", path)
	err = gocsv.Unmarshal(file, &labels)
	if err != nil {
		fmt.Println(err)
	}
	return labels, err
}

func WriteLabelCSV(path string, labels []*db.Label) error {
	file, err := os.Create(path)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Writing timeseries labels to %s...\n", path)
	err = gocsv.Marshal(labels, file)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Dumped %d labels!\n", len(labels))
	}
	return err
}
