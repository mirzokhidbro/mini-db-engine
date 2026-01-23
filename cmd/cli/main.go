package main

import (
	"fmt"
	"os"
	"rdbms/src"
	"rdbms/src/cli"
)

func main() {
	dataDir := "data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}

	stg, err := src.NewStorage(dataDir)
	if err != nil {
		fmt.Println("Error initializing storage:", err)
		os.Exit(1)
	}

	c := cli.NewCLI(stg)
	c.Run()
}
