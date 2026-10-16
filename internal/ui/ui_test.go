package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestHandler(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"index.html":             {Data: []byte("<!doctype html><title>app</title>")},
		"favicon.svg":            {Data: []byte("<svg/>")},
		"assets/index-a1b2.js":   {Data: []byte("console.log(1)")},
		"assets/index-c3d4.css":  {Data: []byte("body{}")},
		"assets/font-e5f6.woff2": {Data: []byte("wOF2")},
	}
	h := NewHandler(fsys)
	tests := []struct {
		method, target string
		status         int
		contentType    string
		cache          string
		body           string
	}{
		{"GET", "/", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"},
		{"GET", "/index.html", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"},
		{"GET", "/p/listings-daily/graph", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"},
		{"GET", "/inbox?demo", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"},
		{"GET", "/assets", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"},
		{"GET", "/assets/index-a1b2.js", 200, "text/javascript; charset=utf-8", immutable, "console.log(1)"},
		{"GET", "/assets/index-c3d4.css", 200, "text/css; charset=utf-8", immutable, "body{}"},
		{"GET", "/assets/font-e5f6.woff2", 200, "font/woff2", immutable, "wOF2"},
		{"GET", "/favicon.svg", 200, "image/svg+xml", "no-cache", "<svg/>"},
		{"GET", "/assets/gone-0000.js", 404, "", "", ""},
		{"GET", "/../../etc/passwd", 200, "text/html; charset=utf-8", "no-cache", "<title>app</title>"}, // cleaned: never leaves fsys
		{"HEAD", "/assets/index-a1b2.js", 200, "text/javascript; charset=utf-8", immutable, ""},
		{"POST", "/", 405, "", "", ""},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, nil))
		if rec.Code != tt.status {
			t.Errorf("%s %s: status %d, want %d", tt.method, tt.target, rec.Code, tt.status)
			continue
		}
		if tt.status != 200 {
			continue
		}
		if got := rec.Header().Get("Content-Type"); got != tt.contentType {
			t.Errorf("%s %s: Content-Type %q, want %q", tt.method, tt.target, got, tt.contentType)
		}
		if got := rec.Header().Get("Cache-Control"); got != tt.cache {
			t.Errorf("%s %s: Cache-Control %q, want %q", tt.method, tt.target, got, tt.cache)
		}
		if got := rec.Body.String(); !strings.Contains(got, tt.body) || (tt.body == "" && got != "") {
			t.Errorf("%s %s: body %q, want %q", tt.method, tt.target, got, tt.body)
		}
	}
}

// Before make ui, dist holds only placeholder.html: every page route serves it.
func TestHandlerPlaceholder(t *testing.T) {
	t.Parallel()
	h := NewHandler(fstest.MapFS{"placeholder.html": {Data: []byte("run make ui")}})
	for _, target := range []string{"/", "/inbox"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != "run make ui" {
			t.Errorf("GET %s: %d %q, want the placeholder", target, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	NewHandler(fstest.MapFS{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("empty dist: status %d, want 500", rec.Code)
	}
}

// The embedded build serves a page whether or not make ui has run.
func TestEmbedded(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("GET /: %d, want the embedded page", rec.Code)
	}
}

func TestContentType(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{"a.woff2": "font/woff2", "a.wasm": "application/wasm", "a.unknownext": "application/octet-stream"} {
		if got := contentType(name); got != want {
			t.Errorf("contentType(%q) = %q, want %q", name, got, want)
		}
	}
}
