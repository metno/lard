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
		progressbar.OptionSetPredictTime(false),
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

// Filters elements of a slice by comparing them to the elements of a reference slice.
// formatMsg is an optional format string with a single format argument that can be used
// to add context on why the element may be missing from the reference slice
func FilterSlice[T comparable](slice, reference []T, formatMsg string) []T {
	if len(slice) == 0 {
		return reference
	}

	if formatMsg == "" {
		formatMsg = "Value '%v' not present in reference slice, skipping"
	}

	// I hate this so much
	out := slice[:0]
	for _, s := range slice {
		if !slices.Contains(reference, s) {
			slog.Warn(fmt.Sprintf(formatMsg, s))
			continue
		}
		out = append(out, s)
	}
	return out
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

func SetLogFile(path, procedure string) {
	filename := fmt.Sprintf("%s/%s_%s.log", path, procedure, time.Now().Format(time.RFC3339))
	fh, err := os.Create(filename)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create log %q: %s", filename, err))
		return
	}
	log.SetOutput(fh)
}

func ToInt32(s string) int32 {
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		// Panic is fine here, because we use this function only at startup
		panic("Could not parse to int")
	}
	return int32(res)
}

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

// Similar to Map, but bails immediately if any error occurs
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
