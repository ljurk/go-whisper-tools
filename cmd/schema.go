package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"github.com/ljurk/yell/lib"
)

var (
	schema    string
	schemaCmd = &cobra.Command{
		Use:   "schema [whisper-dir]",
		Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		Short: "dump schema about whisper file",
		Run: func(cmd *cobra.Command, args []string) {
			path, _ := cmd.Flags().GetString("schema")
			whisperDir := args[0]

			//parse storage-schemas
			schemas, err := lib.ParseStorageSchemas(path)
			if err != nil {
				log.Fatalf("failed to parse schemas %s: %v\n", path, err)
			}
			log.Printf("Found %d schema definitions", len(schemas))

			// find all .wsp files under path
			var files []string
			files, err = lib.FindWhisperFiles(whisperDir)
			if err != nil {
				log.Fatalf("failed walking root %s: %v\n", whisperDir, err)
			}
			if len(files) == 0 {
				log.Fatalf("no .wsp files found under %s\n", whisperDir)
			}

			log.Printf("Found %d whisper files", len(files))

			schemaCounts, _ := lib.CountDefinitions(schemas, whisperDir, files)
			for _, i := range schemaCounts {
				fmt.Printf("[%s] %s > %d\n", i.Definition.Name, i.Definition.Pattern, i.Count)
			}
		},
	}
)

func init() {
	schemaCmd.Flags().StringVar(&schema, "schema", "", "path to storage-schemas.conf")
	err := schemaCmd.MarkFlagRequired("schema")
	if err != nil {
		fmt.Println(err)
	}
	rootCmd.AddCommand(schemaCmd)
}
