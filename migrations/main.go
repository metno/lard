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
	args := CmdArgs{}
	parser := arg.MustParse(&args)

	switch {
	case args.KDVH != nil:
		args.KDVH.Execute(parser)
	case args.Kvalobs != nil:
		args.Kvalobs.Execute(parser)
	default:
		fmt.Print("Error: passing a subcommand is required.\n\n")
		parser.WriteHelp(os.Stdout)
	}
}
