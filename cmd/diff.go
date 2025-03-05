package cmd

import (
	"fmt"

	"github.com/TFMV/flashfs/internal/diff"
	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compare two snapshots",
	RunE: func(cmd *cobra.Command, args []string) error {
		oldFile, _ := cmd.Flags().GetString("old")
		newFile, _ := cmd.Flags().GetString("new")
		if oldFile == "" || newFile == "" {
			return fmt.Errorf("both --old and --new snapshot files must be specified")
		}

		diffs, err := diff.CompareSnapshots(oldFile, newFile)
		if err != nil {
			return err
		}
		for _, d := range diffs {
			fmt.Println(d)
		}
		return nil
	},
}

func init() {
	diffCmd.Flags().String("old", "", "Old snapshot file")
	diffCmd.Flags().String("new", "", "New snapshot file")
	RootCmd.AddCommand(diffCmd)
}
