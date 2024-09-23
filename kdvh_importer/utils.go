package main

import (
	"fmt"
	"log"
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
			log.Printf(formatMsg, s)
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

	insideReference := func(test string) (*Element, bool) {
		for _, element := range reference {
			if test == element.name {
				return &element, true
			}
		}
		return nil, false
	}

	var out []Element
	for _, s := range slice {
		if elem, ok := insideReference(s); ok {
			out = append(out, *elem)
			continue
		}
		log.Printf("Element '%s' not present in database", s)
	}

	return nil
}

func setLogFile(tableName, procedure string) {
	filename := fmt.Sprintf("%s_%s_log.txt", tableName, procedure)
	fh, err := os.Create(filename)
	if err != nil {
		log.Printf("Could not create log '%s': %s", filename, err)
		return
	}
	log.SetOutput(fh)
}
