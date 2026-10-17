package exectest

import (
	"os"
	"testing"

	"github.com/monolithiclab/millstream/internal/testdir"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testdir.Remove()
	os.Exit(code)
}
