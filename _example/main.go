package main

import (
	"os"

	"github.com/arwoosa/log"
)

func main() {
	logConfig := log.NewLogerConfWithFluentd("localhost", 24224)
	os.Setenv("LOG_LEVEL", "info")
	// os.Setenv("LOG_TARGET", "os|fluentd")
	os.Setenv("LOG_TARGET", "os")
	l, err := logConfig.NewLogger("service", "pid")
	if err != nil {
		panic(err)
	}
	l.Error("this is error")
}
