package cmd

import (
	"context"

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

// ExecuteWithContext executes the root command with the given context.
func ExecuteWithContext(ctx context.Context) error {
	// Set the context for the command
	RootCmd.SetContext(ctx)
	return RootCmd.Execute()
}
