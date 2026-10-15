// Command fakeaction is the fake action process for the tests of the exec runtime and of the node that need a real
// process; exectest.Serve documents its modes and runs the same code in-process.
//
// It lives under testdata so the module's ./... patterns skip it; exectest builds it.
package main

import (
	"os"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

func main() { os.Exit(exectest.Main()) }
