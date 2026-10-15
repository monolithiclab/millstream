// Command millstream runs, checks and operates Millstream pipelines.
package main

import (
	"context"
	"os"
	"runtime/debug"

	"github.com/monolithiclab/millstream/internal/cli"
)

// version is set at build time with -X main.version; a plain go install falls back to the module version.
var version = "dev"

func main() {
	os.Exit(cli.Main(context.Background(), os.Args[1:], cli.StdIO(), buildVersion()))
}

func buildVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}
