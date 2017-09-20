package main

import (
	"os"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("eevee")

func main() {
	startLogger()

}

func startLogger() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{color}%{shortfunc} ▶ %{level:.4s} %{color:reset} %{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
