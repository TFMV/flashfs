package main

import (
	"os"

	"github.com/TFMV/flashfs/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
