package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	URL string // the step's notify webhook
	// Secret names the pipeline secret URL was resolved from, empty for a literal URL: the logs then name the
	// secret instead of the webhook's host.
	Secret     string
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

// Payload builds the webhook message for n with links under baseURL. Every value is escaped for Slack's mrkdwn
// (&, < and >), so an item's key or display cannot inject a link or a mention.
func Payload(baseURL string, n Notification) Message {
	link := Link(baseURL, n.Inbox, n.DeliveryID)
	what := "Review needed"
	if n.Escalated {
		what = "Review escalated"
	}
	inbox, pipe, step, key := escape(n.Inbox), escape(n.Pipeline), escape(n.Step), escape(n.ItemKey)
	text := fmt.Sprintf("%s in inbox %s: %s/%s, item %s. %s", what, inbox, pipe, step, key, link)
	if !n.Deadline.IsZero() {
		text += " Decide before " + n.Deadline.UTC().Format(time.RFC3339) + "."
	}
	body := fmt.Sprintf("*%s* in inbox `%s`: %s/%s, item `%s`", what, inbox, pipe, step, key)
	if n.Display != "" {
		body += "\n" + escape(n.Display)
	}
	if len(body) > maxBlockText-100 {
		body = strings.ToValidUTF8(body[:maxBlockText-100], "")
		// A cut must not leave half an entity behind.
		if i := strings.LastIndexByte(body, '&'); i >= 0 && !strings.Contains(body[i:], ";") {
			body = body[:i]
		}
		body += "…"
	}
	body += "\n<" + link + "|Open in the inbox>"
	return Message{Text: text, Blocks: []Block{{Type: "section", Text: BlockText{Type: "mrkdwn", Text: body}}}}
}

// mrkdwn is the escaping Slack asks of text it formats.
var mrkdwn = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

func escape(s string) string { return mrkdwn.Replace(s) }

// webhookName is how logs name n's webhook: never its URL, which may carry a token, but its host, or the secret it
// came from.
func (n Notification) webhookName() string {
	if n.Secret != "" {
		return "[redacted:" + n.Secret + "]"
	}
	if u, err := url.Parse(n.URL); err == nil {
		return u.Host
	}
	return "(malformed URL)"
}

// Notifier posts notifications to review webhooks, in the background: a notification never blocks the engine or a
// decision. Notify queues a notification, at most QueueSize of them, and MaxInFlight workers post them; one that
// finds the queue full is dropped. Each POST is retried Retries times with doubling backoff; a notification that
// still fails, or is dropped, is logged and counted (Failures). The notifier has its own lifetime, apart from any
// request's or engine's context: Shutdown ends it, letting what is queued and in flight finish within a bound.
type Notifier struct {
	// BaseURL is where inbox links point: ui.baseUrl.
	BaseURL string
	// Client posts the notifications; nil uses http.DefaultClient.
	Client *http.Client
	// Sleep waits between retries; nil uses a timer. Tests pass one that returns at once.
	Sleep  func(ctx context.Context, d time.Duration) error
	Logger *slog.Logger
	// QueueSize bounds the notifications waiting for a worker; zero means DefaultQueueSize.
	QueueSize int

	once     sync.Once
	queue    chan Notification
	ctx      context.Context    // the posts' context: cancelled when Shutdown's grace runs out
	cancel   context.CancelFunc //
	retryCtx context.Context    // the retries' context: cancelled as Shutdown starts, so no new retry begins
	noRetry  context.CancelFunc //
	mu       sync.RWMutex       // orders Notify's sends before Shutdown closes the queue
	closed   bool               // under mu
	pending  sync.WaitGroup     // notifications queued or in flight
	workers  sync.WaitGroup
	stopOnce sync.Once
	failures atomic.Int64
}

// MaxInFlight is how many notifications post at once: a burst of parked items against a slow webhook queues
// behind them rather than holding a connection each.
const MaxInFlight = 8

// DefaultQueueSize is how many notifications wait for a worker before Notify drops new ones.
const DefaultQueueSize = 256

// ShutdownGrace is how long an engine shutting down lets the notifications queued and in flight finish.
const ShutdownGrace = 5 * time.Second

func (nt *Notifier) start() {
	nt.once.Do(func() {
		size := nt.QueueSize
		if size <= 0 {
			size = DefaultQueueSize
		}
		nt.queue = make(chan Notification, size)
		nt.ctx, nt.cancel = context.WithCancel(context.Background())
		nt.retryCtx, nt.noRetry = context.WithCancel(nt.ctx)
		for range MaxInFlight {
			nt.workers.Go(nt.work)
		}
	})
}

// work posts queued notifications until Shutdown closes the queue.
func (nt *Notifier) work() {
	for n := range nt.queue {
		if err := nt.send(nt.ctx, nt.retryCtx, n); err != nil {
			nt.fail(n, err)
		}
		nt.pending.Done()
	}
}

// Notify queues n and returns at once. It drops n, counting it as failed, when the queue is full or the notifier
// is shut down.
func (nt *Notifier) Notify(n Notification) {
	nt.start()
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	if nt.closed {
		nt.fail(n, errors.New("dropped: the notifier is shut down"))
		return
	}
	if u, err := url.Parse(n.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		if n.Secret != "" {
			nt.fail(n, fmt.Errorf("secret %s is not set to an http or https URL", n.Secret))
		} else {
			nt.fail(n, errors.New("the webhook is not an http or https URL"))
		}
		return
	}
	nt.pending.Add(1)
	select {
	case nt.queue <- n:
	default:
		nt.pending.Done()
		nt.fail(n, fmt.Errorf("dropped: %d notifications are already queued", cap(nt.queue)))
	}
}

func (nt *Notifier) fail(n Notification, err error) {
	nt.failures.Add(1)
	if nt.Logger != nil {
		nt.Logger.Warn("review notification failed", "pipeline", n.Pipeline, "step", n.Step,
			"deliveryId", n.DeliveryID, "webhook", n.webhookName(), "error", err.Error())
	}
}

// Wait waits until the notifications queued so far are posted or failed.
func (nt *Notifier) Wait() { nt.pending.Wait() }

// Shutdown stops the notifier: Notify drops what comes after, no failed post is retried, and the notifications
// queued and in flight get grace to finish before their posts are cancelled. It returns once every worker has
// stopped; calling it again does nothing.
func (nt *Notifier) Shutdown(grace time.Duration) {
	nt.start()
	nt.stopOnce.Do(func() {
		nt.mu.Lock()
		nt.closed = true
		close(nt.queue)
		nt.mu.Unlock()
		nt.noRetry()
		done := make(chan struct{})
		go func() { nt.workers.Wait(); close(done) }()
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			nt.cancel()
			<-done
		}
		nt.cancel()
	})
}

// Failures is how many notifications failed after their retries or were dropped: the review_notifications_failed
// counter.
func (nt *Notifier) Failures() int64 { return nt.failures.Load() }

// Send posts n, retrying, and returns the last error when every attempt failed.
func (nt *Notifier) Send(ctx context.Context, n Notification) error { return nt.send(ctx, ctx, n) }

// send posts n under ctx, retrying while retryCtx lasts.
func (nt *Notifier) send(ctx, retryCtx context.Context, n Notification) error {
	body, err := json.Marshal(Payload(nt.BaseURL, n))
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
		if serr := sleep(retryCtx, delay); serr != nil {
			return fmt.Errorf("%w (retry abandoned: %w)", err, serr)
		}
		delay *= 2
	}
}

func (nt *Notifier) post(ctx context.Context, target string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil { // a parse error quotes the URL's parts
		return errors.New("the webhook URL does not parse")
	}
	req.Header.Set("Content-Type", "application/json")
	client := nt.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req) // #nosec G107 G704 -- the webhook URL is the pipeline author's notify key
	if err != nil {
		return withoutURL(err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook answered %s", resp.Status)
	}
	return nil
}

// withoutURL drops the URL a *url.Error carries: a webhook URL is a credential, kept out of errors and logs.
func withoutURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
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
