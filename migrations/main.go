package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alexflint/go-arg"

	"migrate/kdvh"
	"migrate/kvalobs"
)

type CmdArgs struct {
	KDVH    *kdvh.Cmd    `arg:"subcommand" help:"Perform KDVH migrations"`
	Kvalobs *kvalobs.Cmd `arg:"subcommand" help:"Perform Kvalobs migrations"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// TODO: this should be printed in the help message
	// The following env variables are required:
	// 1. Dump
	//   - kdvh: "KDVH_PROXY_CONN_STRING"
	//   - kvalobs: "KVALOBS_CONN_STRING", "HISTKVALOBS_CONN_STRING"
	//
	// 2. Import
	//   - kdvh: "LARD_CONN_STRING", "STINFO_CONN_STRING", "KDVH_PROXY_CONN_STRING"
	//   - kvalobs: "LARD_CONN_STRING", "STINFO_CONN_STRING", "KVALOBS_CONN_STRING"
	args := CmdArgs{}
	parser := arg.MustParse(&args)

	switch {
	case args.KDVH != nil:
		args.KDVH.Execute(parser)
	case args.Kvalobs != nil:
		args.Kvalobs.Execute(parser)
	default:
		fmt.Println("Error: passing a subcommand is required.")
		fmt.Println()
		parser.WriteHelp(os.Stdout)
	}
}
