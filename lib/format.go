package lib

import (
	"fmt"
	"strconv"
	"strings"
)

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
	return fmt.Sprintf("%s:%s", ToHuman(spec.SecondsPerPoint), ToHuman(spec.RetentionSecs))
}

func FormatRetentionList(specs []ArchiveSpec) string {
	parts := make([]string, 0, len(specs))
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
