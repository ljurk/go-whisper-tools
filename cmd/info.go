package cmd

import (
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/spf13/cobra"

	yell "github.com/ljurk/yell/lib"
)

var (
	infoCmd = &cobra.Command{
		Use:   "info [whisper-file]",
		Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short: "dump info about whisper file",
		Run: func(cmd *cobra.Command, args []string) {
			path := args[0]
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
					yell.ToHuman(retentionSecs),
					retentionSecs,
				)
			}
			err = wr.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, "error flushing TabWriter")
			}
		},
	}
)

func init() {
	rootCmd.AddCommand(infoCmd)
}
