package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
)

// Filters elements of a slice by comparing them to the elements of a reference slice.
// formatMsg is an optional format string with a single format argument that can be used
// to add context on why the element may be missing from the reference slice
func filterSlice[T comparable](slice, reference []T, formatMsg string) []T {
	if slice == nil {
		return reference
	}

	if formatMsg == "" {
		formatMsg = "User input '%s' not present in reference, skipping"
	}

	var out []T
	for _, s := range slice {
		if !slices.Contains(reference, s) {
			slog.Warn(fmt.Sprintf(formatMsg, s))
			continue
		}
		out = append(out, s)
	}
	return out
}

func filterElements(slice []string, reference []Element) []Element {
	if slice == nil {
		return reference
	}

	findInReference := func(test string) (*Element, bool) {
		for _, element := range reference {
			if test == element.name {
				return &element, true
			}
		}
		return nil, false
	}

	var out []Element
	for _, e := range slice {
		if elem, ok := findInReference(e); ok {
			out = append(out, *elem)
			continue
		}
		slog.Warn(fmt.Sprintf("Element '%s' not present in database", e))
	}

	return nil
}

func setLogFile(tableName, procedure string) {
	filename := fmt.Sprintf("%s_%s_log.txt", tableName, procedure)
	fh, err := os.Create(filename)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create log '%s': %s", filename, err))
		return
	}
	log.SetOutput(fh)
}
