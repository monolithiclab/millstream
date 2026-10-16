// Command uiserve serves a built web UI directory through internal/ui's handler, so the end-to-end tests (make
// test-ui-e2e) exercise the handler the binary ships, until millstream run serves the UI (M3 Task 2).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/monolithiclab/millstream/internal/ui"
)

func main() {
	dir := flag.String("dir", "internal/ui/dist", "the built UI")
	addr := flag.String("addr", "127.0.0.1:4173", "listen address")
	flag.Parse()
	srv := &http.Server{Addr: *addr, Handler: ui.NewHandler(os.DirFS(*dir)), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
