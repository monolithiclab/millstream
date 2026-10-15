package exectest

import (
	"os"
	"testing"
)

func TestBuild(t *testing.T) {
	t.Parallel()
	bin, err := Build(t.TempDir())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if info, err := os.Stat(bin); err != nil || info.Mode()&0o111 == 0 {
		t.Errorf("binary %s: %v, %v", bin, info, err)
	}
}
