package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Set the log file for the logger
func SetLoggerOutput(name, procedure string) *os.File {
	filename := fmt.Sprintf("%s_%s_%s.log", name, procedure, time.Now().Format(time.RFC3339))
	fh, err := os.Create(filename)
	if err != nil {
		log.Error().Err(err).Msg("Could not create file " + filename)
		return nil
	}

	log.Logger = log.Output(fh)
	return fh
}

// Set up the global logger
func InitLogger() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.ErrorFieldName = "err"
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(filepath.Dir(file)) + "/" + filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	log.Logger = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
}
