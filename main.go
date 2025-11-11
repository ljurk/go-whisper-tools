package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"

	whisper "github.com/go-graphite/go-whisper"
)

type ArchiveSpec struct {
	SecondsPerPoint int
	RetentionSecs   int
}

type Schema struct {
	Name       string
	PatternRaw string
	Pattern    *regexp.Regexp
	Retentions []ArchiveSpec
	LineNo     int // ordering preserved; earlier lines have smaller LineNo
}

// toHuman converts seconds into a single-unit short representation used by storage-schemas,
// e.g. 300 -> "300s", 3600 -> "1h", 86400 -> "1d", 31536000 -> "1y"
func toHuman(seconds int) string {
	if seconds == 0 {
		return "0s"
	}
	type unit struct {
		seconds int
		symbol  string
	}

	units := []unit{
		{31536000, "y"},
		{86400, "d"},
		{3600, "h"},
		{60, "m"},
	}

	for _, u := range units {
		if seconds%u.seconds == 0 {
			return fmt.Sprintf("%d%s", seconds/u.seconds, u.symbol)
		}
	}
	return fmt.Sprintf("%ds", seconds)
}

// fromHuman parses strings like "10s", "5m", "2h", "7d", "1y" into seconds.
// Accepts an optional whitespace trimmed string.
// Returns -1 on error.
func fromHuman(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return -1, fmt.Errorf("empty duration")
	}
	// number at front, last rune is unit
	n := len(s)
	unit := s[n-1]
	numStr := s[:n-1]

	val, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric duration in %q", s)
	}
	switch unit {
	case 's', 'S':
		return val, nil
	case 'm', 'M':
		return val * 60, nil
	case 'h', 'H':
		return val * 3600, nil
	case 'd', 'D':
		return val * 86400, nil
	case 'y', 'Y':
		return val * 31536000, nil
	default:
		return -1, fmt.Errorf("unknown duration unit %q in %q", string(unit), s)
	}
}

// formatRetentionList converts a slice of ArchiveSpec into "300s:60d, 1h:2y" style
func (spec ArchiveSpec) toHuman() string {
	return fmt.Sprintf("%s:%s", toHuman(spec.SecondsPerPoint), toHuman(spec.RetentionSecs))
}

func formatRetentionList(specs []ArchiveSpec) string {
	parts := make([]string, len(specs))
	for _, i := range specs {
		parts = append(parts, i.toHuman())
	}
	return strings.Join(parts, ",")
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

// parseStorageSchemas parses a storage-schemas.conf file and returns schemas in file order.
// It supports the typical Graphite format:
//
// [name]
// pattern = REGEX
// retentions = 10s:6h, 1m:7d
//
// Comments starting with # are ignored. The file is processed top-to-bottom and the
// resulting slice preserves ordering so first match wins.
func parseStorageSchemas(path string) ([]Schema, error) {
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

// findWhisperFiles walks root and returns all files ending with .wsp
func findWhisperFiles(root string) ([]string, error) {
	out := []string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip unreadable files/directories
			fmt.Fprintf(os.Stderr, "Skipping %s: %v\n", path, err)
			return nil // <- IMPORTANT: continue walking
			// don't stop on single file errors; but return error if stat fails
			// return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(path), ".wsp") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// metricFromPath converts a filesystem path to Graphite metric name relative to root.
// e.g. /var/lib/graphite/whisper/servers/web01/cpu.wsp -> servers.web01.cpu
func metricFromPath(root, full string) string {
	rel, err := filepath.Rel(root, full)
	if err != nil {
		// fallback to full path turned into dots (not ideal)
		rel = full
	}
	rel = strings.TrimSuffix(rel, ".wsp")
	// on Windows or other OSes, ensure separators are normalized
	rel = strings.TrimPrefix(rel, string(filepath.Separator))
	return strings.ReplaceAll(rel, string(filepath.Separator), ".")
}

// whisperRetentionsToSpecs converts whisper.Retentions() -> []ArchiveSpec preserving order.
func whisperRetentionsToSpecs(retentions []whisper.Retention) []ArchiveSpec {
	out := make([]ArchiveSpec, 0, len(retentions))
	for _, r := range retentions {
		sp := r.SecondsPerPoint()
		points := r.NumberOfPoints()
		total := sp * points
		out = append(out, ArchiveSpec{
			SecondsPerPoint: sp,
			RetentionSecs:   total,
		})
	}
	return out
}

func compareSpecsEqual(a, b []ArchiveSpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].SecondsPerPoint != b[i].SecondsPerPoint || a[i].RetentionSecs != b[i].RetentionSecs {
			return false
		}
	}
	return true
}

func main() {
	shortFlag := flag.Bool("short", false, "print retention in storage-schemas.conf format (e.g. 300s:60d, 1h:2y) for a single file")
	checkFlag := flag.Bool("check-retention", false, "check retentions for all .wsp files under ROOT using the provided storage-schemas.conf")
	schemasPath := flag.String("schemas", "", "path to storage-schemas.conf (required when --check-retention is used)")
	exitOnMismatch := flag.Bool("exit-on-mismatch", true, "exit with non-zero code if any mismatch is found (default true)")
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] path/to/metric.wsp | path/to/whisper_root\n\n", os.Args[0])
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Examples:\n")
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "  %s /var/lib/graphite/whisper/servers.web01.cpu.wsp\n", os.Args[0])
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "  %s --short /var/lib/graphite/whisper/servers.web01.cpu.wsp\n", os.Args[0])
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "  %s --check-retention --schemas=/etc/graphite/storage-schemas.conf /var/lib/graphite/whisper\n", os.Args[0])
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "\nOptions:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	var err error

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	path := flag.Arg(0)

	// single-file short mode
	if *shortFlag && !*checkFlag {
		var w *whisper.Whisper
		w, err = whisper.Open(path)
		if err != nil {
			log.Fatalf("Error opening '%s': %v\n", path, err)
		}
		defer func() {
			err = w.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error closing '%s': %v\n", path, err)
			}
		}()
		specs := whisperRetentionsToSpecs(w.Retentions())
		fmt.Println(formatRetentionList(specs))
		return
	}

	// check-retention mode
	if *checkFlag {
		if *schemasPath == "" {
			log.Fatal("--schemas is required when --check-retention is used")
		}
		var schemas []Schema
		schemas, err = parseStorageSchemas(*schemasPath)
		if err != nil {
			log.Fatalf("failed to parse schemas %s: %v\n", *schemasPath, err)
		}
		// find all .wsp files under path
		var files []string
		files, err = findWhisperFiles(path)
		if err != nil {
			log.Fatalf("failed walking root %s: %v\n", path, err)
		}
		if len(files) == 0 {
			log.Fatalf("no .wsp files found under %s\n", path)
		}

		// output table header
		wr := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(wr, "status\tmetric\texpected\tactual\tdetail")
		mismatchFound := false

		for _, f := range files {
			metric := metricFromPath(path, f)

			// find first matching schema (top-to-bottom)
			var matched *Schema
			for i := range schemas {
				s := &schemas[i]
				// If pattern is empty treat as no-match (Graphite typically has pattern)
				if s.Pattern == nil {
					continue
				}
				if s.Pattern.MatchString(metric) {
					matched = s
					break
				}
			}

			if matched == nil {
				// no schema matched
				_, _ = fmt.Fprintf(wr, "NOMATCH\t%s\t-\t-\tno schema matched\n", metric)
				continue
			}

			// open whisper file and read retentions
			var wf *whisper.Whisper
			wf, err = whisper.Open(f)
			if err != nil {
				_, _ = fmt.Fprintf(wr, "ERROR\t%s\t-\t-\tfailed to open: %v\n", metric, err)
				mismatchFound = true
				continue
			}
			actualSpecs := whisperRetentionsToSpecs(wf.Retentions())
			err = wf.Close()
			if err != nil {
				_, _ = fmt.Fprintf(wr, "ERROR\t%s\t-\t-\tfailed to close: %v\n", path, err)
				return
			}

			expectedSpecs := matched.Retentions

			ok := compareSpecsEqual(actualSpecs, expectedSpecs)
			expectedStr := formatRetentionList(expectedSpecs)
			actualStr := formatRetentionList(actualSpecs)
			if ok {
				_, _ = fmt.Fprintf(wr, "OK\t%s\t%s\t%s\tmatched schema[%s]\n", metric, expectedStr, actualStr, matched.Name)
			} else {
				_, _ = fmt.Fprintf(wr, "MISMATCH\t%s\texpected:%s\tgot:%s\tschema[%s]\n", metric, expectedStr, actualStr, matched.Name)
				mismatchFound = true
			}
		}
		err = wr.Flush()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "ERROR failed to close TabWriter")
			return
		}

		if mismatchFound && *exitOnMismatch {
			os.Exit(1)
		}
		return
	}

	// default: print full info about a single file (table like previous)
	w, err := whisper.Open(path)
	if err != nil {
		log.Fatalf("Error opening '%s': %v\n", path, err)
	}
	defer func() {
		err = w.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error closing file '%s': %v\n", path, err)
		}
	}()

	aggr := w.AggregationMethod().String()
	xff := w.XFilesFactor()
	retentions := w.Retentions()

	fmt.Printf("File: %s\n", path)
	fmt.Printf("Aggregation: %s\n", aggr)
	fmt.Printf("xFilesFactor: %g\n", xff)
	fmt.Println()

	wr := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(wr, "archive\tseconds/point\t#points\tretention\tmax age (sec)")
	for i, r := range retentions {
		secondsPerPoint := r.SecondsPerPoint()
		points := r.NumberOfPoints()
		retentionSecs := secondsPerPoint * points
		_, _ = fmt.Fprintf(wr, "%d\t%d\t%d\t%s\t%d\n",
			i,
			secondsPerPoint,
			points,
			toHuman(retentionSecs),
			retentionSecs,
		)
	}
	err = wr.Flush()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error flushing TabWriter")
	}
}
