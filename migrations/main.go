package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alexflint/go-arg"

	"migrate/index"
	"migrate/kdvh"
	"migrate/kvalobs"
)

type CmdArgs struct {
	KDVH    *kdvh.Cmd     `arg:"subcommand" help:"Perform KDVH migrations"`
	Kvalobs *kvalobs.Cmd  `arg:"subcommand" help:"Perform Kvalobs migrations"`
	Index   *index.Config `arg:"subcommand" help:"Drop or create indices for the LARD tables"`
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
	case args.Index != nil:
		if err := args.Index.Execute(); err != nil {
			fmt.Println(err)
			parser.WriteHelp(os.Stdout)
		}
	default:
		fmt.Print("Error: passing a subcommand is required.\n\n")
		parser.WriteHelp(os.Stdout)
	}
}
