package lib

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	whisper "github.com/go-graphite/go-whisper"
)

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

// findWhisperFiles walks root and returns all files ending with .wsp
func FindWhisperFiles(root string) ([]string, error) {
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
func MetricFromPath(root, full string) string {
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
