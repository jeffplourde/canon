package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffplourde/canon"
	"github.com/jeffplourde/canon/internal/mcp"
)

func main() {
	root := flag.String("root", ".", "folder containing the canon knowledge base")
	flag.Parse()

	store, err := canon.Open(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := store.ScanExternalChanges("mcp-start"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := mcp.New(store).Serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
