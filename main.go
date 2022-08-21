package main

import (
	"os"

	"github.com/filecoin-project/lotus/build"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var (
	log = logging.Logger("sync_filecoin")
)

func main() {
	if err := logging.SetLogLevel("*", "info"); err != nil {
		log.Fatal(err)
	}
	app := &cli.App{
		Name:    "sync_filecoin",
		Usage:   "",
		Version: build.UserVersion(),
		Flags:   []cli.Flag{},
		Commands: []*cli.Command{
			cmdInitDb,
			cmdPorter,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
