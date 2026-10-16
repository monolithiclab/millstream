package nodeapi

import (
	"os"
	"path/filepath"
)

// WorkRoot picks where a remote node creates its deliveries' working directories (spec section 5.10: secrets stay
// in memory and in the delivery's working directory, never on disk): the first of $XDG_RUNTIME_DIR, /dev/shm and the
// system's temporary directory that is a tmpfs and writable, else the system's temporary directory. Either way each
// working directory is created 0700 and removed when its process ends; tmpfs reports whether it is memory-backed
// (only Linux tells).
func WorkRoot(env func(string) (string, bool)) (dir string, tmpfs bool) {
	var candidates []string
	if d, ok := env("XDG_RUNTIME_DIR"); ok && d != "" {
		candidates = append(candidates, d)
	}
	candidates = append(candidates, "/dev/shm", os.TempDir())
	for _, d := range candidates {
		if isTmpfs(d) && writable(d) {
			return d, true
		}
	}
	return os.TempDir(), false
}

func writable(dir string) bool {
	f, err := os.CreateTemp(dir, ".millstream-probe-")
	if err != nil {
		return false
	}
	_ = f.Close()
	return os.Remove(filepath.Clean(f.Name())) == nil
}
