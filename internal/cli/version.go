package cli

import "fmt"

// VersionCmd is "millstream version".
type VersionCmd struct{}

// Run prints the version on stdout.
func (VersionCmd) Run(rt *Runtime) error {
	_, err := fmt.Fprintf(rt.IO.Out, "millstream %s\n", rt.Version)
	return err
}
