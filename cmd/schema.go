package cmd

import (
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/spf13/cobra"

	"github.com/ljurk/yell/lib"
)

var (
	schema    string
	schemaCmd = &cobra.Command{
		Use:   "schema",
		Short: "command to run analysis in comparison to a storage-schemas.conf",
	}
	checkCmd = &cobra.Command{
		Use:   "check [whisper-dir]",
		Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short: "check if whisper files are matching the defined retentions",
		Run: func(cmd *cobra.Command, args []string) {
			path, _ := cmd.Flags().GetString("schema")
			//parse storage-schemas
			schemas, err := lib.ParseStorageSchemas(path)
			if err != nil {
				log.Fatalf("failed to parse schemas %s: %v\n", path, err)
			}

			// find all .wsp files under path
			var files []string
			files, err = lib.FindWhisperFiles(args[0])
			if err != nil {
				log.Fatalf("failed walking root %s: %v\n", args[0], err)
			}
			if len(files) == 0 {
				log.Fatalf("no .wsp files found under %s\n", args[0])
			}

			// output table header
			wr := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(wr, "status\tmetric\texpected\tactual\tdetail")

			for _, f := range files {
				metric := lib.MetricFromPath(args[0], f)

				// find first matching schema (top-to-bottom)
				var matched *lib.Schema
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
					continue
				}
				actualSpecs := lib.WhisperRetentionsToSpecs(wf.Retentions())
				err = wf.Close()
				if err != nil {
					_, _ = fmt.Fprintf(wr, "ERROR\t%s\t-\t-\tfailed to close: %v\n", path, err)
					return
				}

				expectedStr := lib.FormatRetentionList(matched.Retentions)
				actualStr := lib.FormatRetentionList(actualSpecs)
				if lib.CompareSpecsEqual(actualSpecs, matched.Retentions) {
					_, _ = fmt.Fprintf(wr, "OK\t%s\t%s\t%s\tmatched schema[%s]\n", metric, expectedStr, actualStr, matched.Name)
				} else {
					_, _ = fmt.Fprintf(wr, "MISMATCH\t%s\texpected:%s\tgot:%s\tschema[%s]\n", metric, expectedStr, actualStr, matched.Name)
				}
			}
			err = wr.Flush()
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "ERROR failed to close TabWriter")
			}
		},
	}
	countCmd = &cobra.Command{
		Use:   "count [whisper-dir]",
		Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short: "count matching metrics per definition",
		Run: func(cmd *cobra.Command, args []string) {
			path, _ := cmd.Flags().GetString("schema")

			//parse storage-schemas
			schemas, err := lib.ParseStorageSchemas(path)
			if err != nil {
				log.Fatalf("failed to parse schemas %s: %v\n", path, err)
			}
			log.Printf("Found %d schema definitions", len(schemas))

			// find all .wsp files under path
			var files []string
			files, err = lib.FindWhisperFiles(args[0])
			if err != nil {
				log.Fatalf("failed walking root %s: %v\n", args[0], err)
			}
			if len(files) == 0 {
				log.Fatalf("no .wsp files found under %s\n", args[0])
			}

			log.Printf("Found %d whisper files", len(files))

			schemaCounts, _ := lib.CountDefinitions(schemas, args[0], files)
			for _, i := range schemaCounts {
				fmt.Printf("[%s] %s > %d\n", i.Definition.Name, i.Definition.Pattern, i.Count)
			}
		},
	}
)

func init() {
	schemaCmd.PersistentFlags().StringVarP(&schema, "schema", "s", "", "path to storage-schemas.conf")
	_ = schemaCmd.MarkPersistentFlagRequired("schema")

	schemaCmd.AddCommand(countCmd)
	schemaCmd.AddCommand(checkCmd)
	rootCmd.AddCommand(schemaCmd)
}
