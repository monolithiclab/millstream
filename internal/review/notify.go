package review

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultBaseURL is where inbox links point without ui.baseUrl in the environment file: the UI millstream run serves.
const DefaultBaseURL = "http://127.0.0.1:7070"

// Notification retries: a failed POST is retried this many times, after Backoff, doubling.
const (
	Retries       = 3
	Backoff       = time.Second
	notifyTimeout = 10 * time.Second
	// maxBlockText is Slack's limit on a section block's text.
	maxBlockText = 3000
)

// Notification is one parked item to tell a review step's webhook about.
type Notification struct {
	URL        string // the step's notify webhook
	Pipeline   string
	Step       string
	Inbox      string
	DeliveryID string
	ItemKey    string
	Display    string
	// Escalated is set when a timeout moved the item to Inbox.
	Escalated bool
	Deadline  time.Time // zero without a timeout
}

// Message is the webhook's JSON body, compatible with Slack incoming webhooks: text, and blocks for clients that
// render them.
type Message struct {
	Text   string  `json:"text"`
	Blocks []Block `json:"blocks,omitempty"`
}

// Block is a Slack section block.
type Block struct {
	Type string    `json:"type"`
	Text BlockText `json:"text"`
}

// BlockText is a block's mrkdwn text.
type BlockText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Link is the inbox link of a parked delivery: <baseURL>/inbox/<inbox>/<deliveryID>.
func Link(baseURL, inbox, deliveryID string) string {
	return strings.TrimSuffix(baseURL, "/") + "/inbox/" + url.PathEscape(inbox) + "/" + url.PathEscape(deliveryID)
}

// Payload builds the webhook message for n with links under baseURL.
func Payload(baseURL string, n Notification) Message {
	link := Link(baseURL, n.Inbox, n.DeliveryID)
	what := "Review needed"
	if n.Escalated {
		what = "Review escalated"
	}
	text := fmt.Sprintf("%s in inbox %s: %s/%s, item %s. %s", what, n.Inbox, n.Pipeline, n.Step, n.ItemKey, link)
	if !n.Deadline.IsZero() {
		text += " Decide before " + n.Deadline.UTC().Format(time.RFC3339) + "."
	}
	body := fmt.Sprintf("*%s* in inbox `%s`: %s/%s, item `%s`", what, n.Inbox, n.Pipeline, n.Step, n.ItemKey)
	if n.Display != "" {
		body += "\n" + n.Display
	}
	if len(body) > maxBlockText-100 {
		body = strings.ToValidUTF8(body[:maxBlockText-100], "") + "…"
	}
	body += "\n<" + link + "|Open in the inbox>"
	return Message{Text: text, Blocks: []Block{{Type: "section", Text: BlockText{Type: "mrkdwn", Text: body}}}}
}

// Notifier posts notifications to review webhooks, in the background: a notification never blocks the engine or a
// decision. Each POST is retried Retries times with doubling backoff; a notification that still fails is logged and
// counted (Failures).
type Notifier struct {
	// BaseURL is where inbox links point: ui.baseUrl, DefaultBaseURL when empty.
	BaseURL string
	// Client posts the notifications; nil uses http.DefaultClient.
	Client *http.Client
	// Sleep waits between retries; nil uses a timer. Tests pass one that returns at once.
	Sleep  func(ctx context.Context, d time.Duration) error
	Logger *slog.Logger

	wg       sync.WaitGroup
	failures atomic.Int64
	slots    chan struct{} // bounds the posts in flight; made on first use
	once     sync.Once
}

// MaxInFlight is how many notifications post at once: a burst of parked items against a slow webhook queues
// behind them rather than holding a connection each.
const MaxInFlight = 8

// Notify sends n in the background and returns at once.
func (nt *Notifier) Notify(ctx context.Context, n Notification) {
	nt.once.Do(func() { nt.slots = make(chan struct{}, MaxInFlight) })
	nt.wg.Go(func() {
		select {
		case nt.slots <- struct{}{}:
			defer func() { <-nt.slots }()
		case <-ctx.Done():
			nt.failures.Add(1)
			return
		}
		if err := nt.Send(ctx, n); err != nil {
			nt.failures.Add(1)
			if nt.Logger != nil {
				nt.Logger.Warn("review notification failed", "pipeline", n.Pipeline, "step", n.Step,
					"deliveryId", n.DeliveryID, "error", err.Error())
			}
		}
	})
}

// Wait waits for the notifications in flight.
func (nt *Notifier) Wait() { nt.wg.Wait() }

// Failures is how many notifications failed after their retries: the review_notifications_failed counter.
func (nt *Notifier) Failures() int64 { return nt.failures.Load() }

// Send posts n, retrying, and returns the last error when every attempt failed.
func (nt *Notifier) Send(ctx context.Context, n Notification) error {
	base := nt.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	body, err := json.Marshal(Payload(base, n))
	if err != nil {
		return err
	}
	sleep := nt.Sleep
	if sleep == nil {
		sleep = timerSleep
	}
	delay := Backoff
	for attempt := 0; ; attempt++ {
		if err = nt.post(ctx, n.URL, body); err == nil {
			return nil
		}
		if attempt == Retries {
			return fmt.Errorf("after %d attempts: %w", attempt+1, err)
		}
		if serr := sleep(ctx, delay); serr != nil {
			return fmt.Errorf("%w (retry abandoned: %w)", err, serr)
		}
		delay *= 2
	}
}

func (nt *Notifier) post(ctx context.Context, target string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := nt.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req) // #nosec G107 G704 -- the webhook URL is the pipeline author's notify key
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook answered %s", resp.Status)
	}
	return nil
}

func timerSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
