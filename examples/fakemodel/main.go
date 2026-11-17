// Command fakemodel is a stand-in for an OpenAI-compatible model, on http://127.0.0.1:8099/v1: it scores the listing
// a prompt describes from its price per square metre, so an agent step can be recorded without a model or a key.
// The getting-started guide records its agent step against it (go run ./examples/fakemodel), and the listings
// example's recordings come from it (make recordings).
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

var listing = regexp.MustCompile(`Listing (\w+) in (\w+): (\d+) m², condition (\w+), asking (\d+) EUR`)

// handler answers chat completions: the listing as the prompt gives it, with a score that is higher the cheaper the
// square metre, in a fenced JSON block.
func handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 {
			http.Error(w, "not a chat completion", http.StatusBadRequest)
			return
		}
		m := listing.FindStringSubmatch(req.Messages[0].Content)
		if m == nil {
			http.Error(w, "no listing in the prompt", http.StatusBadRequest)
			return
		}
		surface, _ := strconv.Atoi(m[3])
		price, _ := strconv.Atoi(m[5])
		perM2 := price / max(surface, 1)
		score := max(0, min(100, 100-(perM2-3000)/20)) // cheaper per square metre scores higher
		answer, _ := json.Marshal(map[string]any{
			"id": m[1], "city": m[2], "surfaceM2": surface, "condition": m[4], "priceEur": price,
			"score":  score,
			"reason": fmt.Sprintf("%d EUR per square metre in %s.", perM2, m[2]),
		})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": req.Model,
			"choices": []map[string]any{{"index": 0, "finish_reason": "stop",
				"message": map[string]string{"role": "assistant", "content": "```json\n" + string(answer) + "\n```"}}},
			"usage": map[string]int{"prompt_tokens": 300, "completion_tokens": 60},
		})
	})
	return mux
}

func main() {
	srv := &http.Server{Addr: "127.0.0.1:8099", Handler: handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Println("fake model on http://127.0.0.1:8099/v1")
	log.Fatal(srv.ListenAndServe())
}
