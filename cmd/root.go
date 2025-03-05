package cmd

import (
	"github.com/spf13/cobra"
)

// RootCmd is the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "snaptool",
	Short: "FlashFS Snapshot Tool",
	Long: `FlashFS is a high-performance, concurrent filesystem snapshot tool.
It efficiently scans and records filesystem structure and metadata into FlatBuffers.`,
}

// Execute executes the root command.
func Execute() error {
	return RootCmd.Execute()
}
