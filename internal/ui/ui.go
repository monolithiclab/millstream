// Package ui serves the web UI (spec section 12.2): the Vite build of ui/, written into dist/ by make ui and
// embedded at compile time. A binary built before make ui embeds only the committed dist/placeholder.html, which
// says how to build the UI, so the Go build never needs npm.
package ui

import (
	"embed"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed all:dist
var dist embed.FS

const (
	index       = "index.html"
	placeholder = "placeholder.html"
	// Vite fingerprints everything under assets/: a changed file gets a new name, so it may be cached for good.
	immutable  = "public, max-age=31536000, immutable"
	revalidate = "no-cache"
)

// Explicit, so a host's mime.types cannot change what the binary serves.
var contentTypes = map[string]string{
	".css":         "text/css; charset=utf-8",
	".html":        "text/html; charset=utf-8",
	".ico":         "image/x-icon",
	".js":          "text/javascript; charset=utf-8",
	".json":        "application/json",
	".map":         "application/json",
	".png":         "image/png",
	".svg":         "image/svg+xml",
	".txt":         "text/plain; charset=utf-8",
	".webmanifest": "application/manifest+json",
	".woff":        "font/woff",
	".woff2":       "font/woff2",
}

// Handler serves the embedded UI.
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // "dist" is a valid constant path: unreachable
	}
	return NewHandler(sub)
}

// NewHandler serves the single-page application in fsys: files as they are, with immutable caching for the hashed
// assets, and index.html (placeholder.html when the UI is not built) for every other path without an extension,
// so client-side routes such as /p/listings-daily/graph load the app. Paths with an extension that match no file
// are 404. The router mounts the API ahead of this handler, with its own 404s.
func NewHandler(fsys fs.FS) http.Handler {
	page := index
	if _, err := fs.Stat(fsys, index); err != nil {
		page = placeholder
	}
	return &handler{fsys: fsys, page: page}
}

type handler struct {
	fsys fs.FS
	page string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name != "" && name != index {
		cache := revalidate
		if strings.HasPrefix(name, "assets/") {
			cache = immutable
		}
		if h.serve(w, r, name, cache) {
			return
		}
		if path.Ext(name) != "" {
			http.NotFound(w, r)
			return
		}
	}
	if !h.serve(w, r, h.page, revalidate) {
		http.Error(w, "web UI missing from this build", http.StatusInternalServerError)
	}
}

// serve writes the file name, opened once (embedded and directory files seek, so ServeContent reads them in place),
// and reports false when it is not a regular file.
func (h *handler) serve(w http.ResponseWriter, r *http.Request, name, cache string) bool {
	f, err := h.fsys.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	content, seekable := f.(io.ReadSeeker)
	if err != nil || info.IsDir() || !seekable {
		return false
	}
	header := w.Header()
	header.Set("Content-Type", contentType(name))
	header.Set("Cache-Control", cache)
	header.Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, name, time.Time{}, content)
	return true
}

func contentType(name string) string {
	ext := path.Ext(name)
	if t, ok := contentTypes[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}
