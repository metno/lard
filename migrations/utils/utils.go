package utils

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
)

// Create a new progress bar
func NewBar(size int, description string) *progressbar.ProgressBar {
	return progressbar.NewOptions(size,
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
		progressbar.OptionSetDescription(description),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

// Check if the first argument is an empty string or if it's equal to the second argument
func StringIsEmptyOrEqual(first, second string) bool {
	return first == "" || first == second
}

// Saves a slice to a file
func SaveToFile(values []string, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	file.WriteString(strings.Join(values, "\n"))
	return file.Close()
}

func SetLogFile(name, procedure string) *os.File {
	filename := fmt.Sprintf("%s_%s_%s.log", name, procedure, time.Now().Format(time.RFC3339))
	fh, err := os.Create(filename)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create log %q: %s", filename, err))
		return nil
	}
	log.SetOutput(fh)
	return fh
}

func ToInt32(s string) int32 {
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		// Panic is fine here, because we use this function only at startup
		panic("Could not parse to int")
	}
	return int32(res)
}

// Maps function `fn` to each element of the slice `ts`, but bails immediately if any error occurs
func TryMap[T, V any](ts []T, fn func(T) (V, error)) ([]V, error) {
	result := make([]V, len(ts))
	for i, t := range ts {
		temp, err := fn(t)
		if err != nil {
			return nil, err
		}
		result[i] = temp
	}
	return result, nil
}

// Returns `true` if the slice is nil, otherwise checks if the element is
// contained in the slice
func IsNilOrContains[T comparable](s []T, v T) bool {
	if s == nil {
		return true
	}
	return slices.Contains(s, v)
}

// Returns `true` if the slice is nil,
// `false` if the element pointer is nil,
// otherwise checks if the element is contained in the slice
func IsNilOrContainsPtr[T comparable](s []T, v *T) bool {
	if s == nil {
		return true
	}

	if v == nil {
		// Nil value is definitely not contained in non-nil slice
		return false
	}

	return slices.Contains(s, *v)
}

// If the element pointer is not nil, checks if the element is in the slice.
// Otherwise it returns `false`
func ContainsPtr[T comparable](s []T, v *T) bool {
	if v == nil {
		// Nil value is not contained in slice
		return false
	}

	return slices.Contains(s, *v)
}
