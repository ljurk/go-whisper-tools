package lib

import (
	"regexp"
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
