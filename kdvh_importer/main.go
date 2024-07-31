package main

import (
	"log"
	"slices"

	"github.com/joho/godotenv"
)

func migrationStep(config *MigrationConfig, step func(*TableInstructions, *MigrationConfig)) {
	for name, table := range TABLE2INSTRUCTIONS {
		// Skip tables not in table list
		if config.Tables != nil && !slices.Contains(config.Tables, name) {
			continue
		}
		step(table, config)
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln(err)
	}

	args, err := processArgs()
	if err != nil {
		return
	}

	opts := NewMigrationConfig(args)
	if args.Import {
		err := opts.cacheMetadata()
		if err != nil {
			log.Fatalln("Could not load metadata from stinfosys:", err)
		}
		migrationStep(opts, importTable)
	}

	log.Println("KDVH importer finished without errors.")
	// SendEmail("ODA – KDVH importer finished running", "KDVH importer completed without fatal errors!")
}
