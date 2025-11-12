package lib

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type SchemaCount struct {
	Definition Schema
	Count      int
}

func CountDefinitions(schemas []Schema, whisperDir string, files []string) ([]SchemaCount, error) {
	// create count list
	counts := make([]SchemaCount, 0, len(schemas))
	for _, i := range schemas {
		counts = append(counts, SchemaCount{
			Definition: i,
			Count:      0,
		})
	}

	for _, f := range files {
		metric := MetricFromPath(whisperDir, f)

		// find first matching schema (top-to-bottom)
		for i := range counts {
			s := &counts[i]
			// If pattern is empty treat as no-match (Graphite typically has pattern)
			if s.Definition.Pattern == nil {
				continue
			}
			if s.Definition.Pattern.MatchString(metric) {
				s.Count += 1
				break
			}
		}
	}
	return counts, nil
}

// ParseStorageSchemas parses a storage-schemas.conf file and returns schemas in file order.
// It supports the typical Graphite format:
//
// [name]
// pattern = REGEX
// retentions = 10s:6h, 1m:7d
//
// Comments starting with # are ignored. The file is processed top-to-bottom and the
// resulting slice preserves ordering so first match wins.
func ParseStorageSchemas(path string) ([]Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() {
		err := f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close file %s %v\n", path, err)
		}
	}()

	scanner := bufio.NewScanner(f)
	var schemas []Schema
	var curName string
	var curPattern string
	var curRetentions string
	lineNo := 0
	sectionLine := 0

	flushSection := func() error {
		if curName == "" {
			return nil
		}
		if curPattern == "" && curRetentions == "" {
			// empty section: ignore
			curName = ""
			return nil
		}
		var compiled *regexp.Regexp
		if curPattern != "" {
			re, err := regexp.Compile(curPattern)
			if err != nil {
				return fmt.Errorf("failed compiling pattern %q in section [%s]: %v", curPattern, curName, err)
			}
			compiled = re
		}
		var retSpecs []ArchiveSpec
		if curRetentions != "" {
			rs, err := parseRetentionList(curRetentions)
			if err != nil {
				return fmt.Errorf("failed parsing retentions in section [%s]: %v", curName, err)
			}
			retSpecs = rs
		}
		schemas = append(schemas, Schema{
			Name:       curName,
			PatternRaw: curPattern,
			Pattern:    compiled,
			Retentions: retSpecs,
			LineNo:     sectionLine,
		})
		curName = ""
		curPattern = ""
		curRetentions = ""
		return nil
	}

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trim := strings.TrimSpace(line)
		// strip comments starting with #
		if i := strings.Index(trim, "#"); i >= 0 {
			trim = strings.TrimSpace(trim[:i])
		}
		if trim == "" {
			continue
		}
		// section header
		if strings.HasPrefix(trim, "[") && strings.HasSuffix(trim, "]") {
			// flush previous
			if err := flushSection(); err != nil {
				return nil, err
			}
			curName = strings.TrimSpace(trim[1 : len(trim)-1])
			sectionLine = lineNo
			continue
		}
		// key = value lines
		if eq := strings.Index(trim, "="); eq >= 0 {
			key := strings.TrimSpace(trim[:eq])
			val := strings.TrimSpace(trim[eq+1:])
			switch strings.ToLower(key) {
			case "pattern":
				curPattern = val
			case "retentions":
				curRetentions = val
			default:
				// ignore other keys
			}
		}
	}
	// flush last
	if err := flushSection(); err != nil {
		return nil, err
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return schemas, nil
}

// parseRetentionList parses a string like "10s:6h, 1m:7d" into []ArchiveSpec (in the same order)
func parseRetentionList(s string) ([]ArchiveSpec, error) {
	out := []ArchiveSpec{}
	// split by comma, but be tolerant of spaces
	for p := range strings.SplitSeq(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		spec, err := parseRetentionSpec(p)
		if err != nil {
			return nil, err
		}
		out = append(out, spec)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no retentions parsed from %q", s)
	}
	return out, nil
}

// parseRetentionSpec parses one "resolution:retention" pair like "10s:6h"
func parseRetentionSpec(pair string) (ArchiveSpec, error) {
	parts := strings.Split(pair, ":")
	if len(parts) != 2 {
		return ArchiveSpec{}, fmt.Errorf("invalid retention pair %q", pair)
	}
	resS, err := fromHuman(strings.TrimSpace(parts[0]))
	if err != nil {
		return ArchiveSpec{}, fmt.Errorf("invalid resolution in %q: %v", pair, err)
	}
	retS, err := fromHuman(strings.TrimSpace(parts[1]))
	if err != nil {
		return ArchiveSpec{}, fmt.Errorf("invalid retention in %q: %v", pair, err)
	}
	// retention must be an integer multiple of resolution ideally, but we'll not enforce that strictly.
	return ArchiveSpec{
		SecondsPerPoint: resS,
		RetentionSecs:   retS,
	}, nil
}
