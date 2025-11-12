package lib

import (
	"fmt"
	"strings"

	whisper "github.com/go-graphite/go-whisper"
)

type ArchiveSpec struct {
	SecondsPerPoint int
	RetentionSecs   int
}

func WhisperRetentionsToSpecs(retentions []whisper.Retention) []ArchiveSpec {
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

// formatRetentionList converts a slice of ArchiveSpec into "300s:60d, 1h:2y" style
func (spec ArchiveSpec) toHuman() string {
	return fmt.Sprintf("%s:%s", ToHuman(spec.SecondsPerPoint), ToHuman(spec.RetentionSecs))
}

func FormatRetentionList(specs []ArchiveSpec) string {
	parts := make([]string, len(specs))
	for _, i := range specs {
		parts = append(parts, i.toHuman())
	}
	return strings.Join(parts, ",")
}

// toHuman converts seconds into a single-unit short representation used by storage-schemas,
// e.g. 300 -> "300s", 3600 -> "1h", 86400 -> "1d", 31536000 -> "1y"
func ToHuman(seconds int) string {
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
