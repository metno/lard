package utils

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/schollz/progressbar/v3"
)

func GoMemLimitMessage(packageName string) {
	if os.Getenv("GOMEMLIMIT") == "" {
		fmt.Println("To avoid OOM kills, set the GOMEMLIMIT environement variable.")
		fmt.Println("For example:")
		fmt.Printf("\tGOMEMLIMIT=4GiB ./migrate %s import ...\n", packageName)
		os.Exit(0)
	}
}

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

// Loads a slice from a file
func LoadFromFile(fh *os.File) (out []string, err error) {
	scanner := bufio.NewScanner(fh)

	for scanner.Scan() {
		out = append(out, scanner.Text())
	}

	err = scanner.Err()
	return out, err
}

func Atoi32(s string) (int32, error) {
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(res), nil
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
