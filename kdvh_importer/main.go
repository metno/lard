package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"runtime/debug"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

type CmdArgs struct {
	BaseDir      string `long:"dir" default:"./" description:"base directory where the dumped data is stored"`
	Sep          string `long:"sep" default:";"  description:"the value separator in the dumped files"`
	DataTable    string `long:"table" default:"" description:"Optional comma separated list of data table names to import. By default all available tables are imported"`
	StnrList     string `long:"stnr" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are imported"`
	ElemCodeList string `long:"elemcode" default:"" description:"Optional comma separated list of element codes.  By default all element codes are imported"`
	Import       bool   `long:"import" description:"Import the given combined data"`

	// TODO: These might need to be implemented later, right now we are only focusing on importing already dumped tables
	// ImportAll    bool   `long:"importall" description:"Import all combined table directories"`
	// Dump               bool   `long:"dump" description:"(optional, for method 'data') – if given, data will be dumped from KDVH"`
	// DumpAll            bool   `long:"dumpall" description:"(optional, for method 'data') – if given, data will be dumped from KDVH, performed for all tables missing a folder"`
	// SkipData           bool   `long:"skipdatadump" description:"skip data table – if given, the values from dataTable will NOT be processed"`
	// SkipFlags          bool   `long:"skipflagdump" description:"skip flag table – if given, the values from flagTable will NOT be processed"`
	// Limit              string `long:"limit" default:"" description:"limit – if given, the procedure will stop after migrating this number of stations"`
	// SwitchTableType    string `long:"switch" choice:"default" choice:"fetchkdvh" description:"perform source switch, can be 'default' or 'fetchkdvh'"`
	// SwitchWholeTable   bool   `long:"switchtable" description:"source switch all timeseries – if defined together with switch, this will switch all timeseries in table, not just those found in datadir"`
	// SwitchAll          bool   `long:"switchall" description:"source switch all timeseries – if given together with switch, this will run a type switch for all timeseries of all data tables that have a combined folder"`
	// Validate           bool   `long:"validate" description:"perform data validation – if given, imported data will be validated against KDVH"`
	// ValidateAll        bool   `long:"validateall" description:"validate all timeseries – if defined, this will run validation for all data tables that have a combined folder"`
	// ValidateWholeTable bool   `long:"validatetable" description:"validate all timeseries – if defined together with validate, this will compare ODA with all KDVH timeseries, not just those found in datadir"`
	// Overwrite          bool   `long:"overwrite" description:"overwrite files – if given, then any existing dumped files will be overwritten"`

	// TODO:Can also use subcommands
	// DataCmd struct {
	//    ...
	// } `command:"data"`
	//
	// NormalsCmd struct {
	//    ...
	// } `command:"normals"`
	//
	// or:
	// DumpCmd struct {
	//    ...
	// } `command:"dump"`
	//
	// or:
	// ImportCmd struct {
	//    ...
	// } `command:"import"`
}

// global list of email recipients
var recipients []string

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalln(err)
	}

	args := CmdArgs{}
	_, err = flags.Parse(&args)

	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok {
			if flagsErr.Type == flags.ErrHelp {
				return
			}
		}
		fmt.Println("See 'kdvh_importer -h' for help")
		return
	}

	opts := NewMigrationConfig(&args)
	if args.Import {
		migrationStep(opts, importTable)
	}

	log.Println("KDVH importer finished without errors.")
	// SendEmail("ODA – KDVH importer finished running", "KDVH importer completed without fatal errors!")
}

func SendEmail(subject, body string) {
	// server and from/to
	host := "aspmx.l.google.com"
	port := "25"
	from := "oda-noreply@met.no"
	to := recipients

	// add stuff to headers and make the message body
	header := make(map[string]string)
	header["From"] = from
	header["To"] = strings.Join(to, ",")
	header["Subject"] = subject
	header["MIME-Version"] = "1.0"
	header["Content-Type"] = "text/plain; charset=\"utf-8\""
	header["Content-Transfer-Encoding"] = "base64"
	message := ""
	for k, v := range header {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}

	body = body + "\n\n" + fmt.Sprintf("Ran with the following command:\n%s", strings.Join(os.Args, " "))
	message += "\r\n" + base64.StdEncoding.EncodeToString([]byte(body))

	// send the email
	err := smtp.SendMail(host+":"+port, nil, from, to, []byte(message))
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Email sent successfully!")
}

// send an email and resume the panic
func EmailOnPanic(function string) {
	if r := recover(); r != nil {
		body := "KDVH importer was unable to finish successfully, and the error was not handled. This email is sent from a recover function triggered in " + function + ".\n\nError message:" + fmt.Sprint(r) + "\n\nStack trace:\n\n" + string(debug.Stack())
		SendEmail("ODA – KDVH importer panicked", body)
		panic(r)
	}
}
