package utils

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/smtp"
	"os"
	"runtime/debug"
	"strings"
)

func sendEmail(subject, body string, to []string) {
	// server and from/to
	host := "aspmx.l.google.com"
	port := "25"
	from := "oda-noreply@met.no"

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
		slog.Error(err.Error())
		return
	}
	slog.Info("Email sent successfully!")
}

// TODO: modify this to be more flexible
// send an email and resume the panic
func SendEmailOnPanic(function string, recipients []string) {
	if r := recover(); r != nil {
		if recipients != nil {
			body := "KDVH importer was unable to finish successfully, and the error was not handled." +
				" This email is sent from a recover function triggered in " +
				function +
				".\n\nError message:" +
				fmt.Sprint(r) +
				"\n\nStack trace:\n\n" +
				string(debug.Stack())
			sendEmail("LARD – KDVH importer panicked", body, recipients)
		}
		panic(r)
	}
}
