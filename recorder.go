package mcpreplay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MakeIngestPath returns the ingest endpoint path for an app ID ("org/app").
func MakeIngestPath(appID string) string {
	org, app, _ := strings.Cut(appID, "/")
	return fmt.Sprintf("/v1/orgs/%s/apps/%s/mcp/events", org, app)
}

// ParseAppID extracts "org/app" from a LogRocket API key, or returns "" if
// the key is not in a recognized format.
func ParseAppID(apiKey string) string {
	parts := strings.Split(apiKey, ":")

	var org, app string
	switch {
	case len(parts) == 4 && parts[0] == "pat":
		org, app = parts[1], parts[2]
	case len(parts) == 3:
		org, app = parts[0], parts[1]
	}

	if org == "" || app == "" {
		return ""
	}
	return org + "/" + app
}

// Recorder buffers events and uploads them from a background goroutine so
// the host server's request handling is never blocked.
type Recorder struct {
	opts       Options
	instanceID string
	appID      string

	emptyUserIDOnce sync.Once

	mu         sync.Mutex
	cond       *sync.Cond
	buffer     []*RequestEvent
	seq        int64
	flushTimer *time.Timer
	pending    []*EventBatch
	working    bool
}

// NewRecorder creates a recorder. Recording is disabled (and OnError called)
// when an app ID cannot be parsed from opts.APIKey.
func NewRecorder(opts Options) *Recorder {
	r := &Recorder{
		opts:       opts,
		instanceID: rand.Text(),
		appID:      ParseAppID(opts.APIKey),
	}
	r.cond = sync.NewCond(&r.mu)

	if r.appID == "" {
		r.reportError(errors.New("could not parse an app ID from APIKey; recording is disabled"))
	}
	return r
}

// InstanceID is the random ID stamped on every event this recorder buffers.
func (r *Recorder) InstanceID() string { return r.instanceID }

// AppID is the "org/app" parsed from the API key, or "" if recording is disabled.
func (r *Recorder) AppID() string { return r.appID }

func (r *Recorder) reportError(err error) {
	if r.opts.OnError == nil {
		return
	}
	defer func() { _ = recover() }()
	r.opts.OnError(err)
}

// NextSeq returns the next per-recorder event sequence number.
func (r *Recorder) NextSeq() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	return r.seq
}

// Enqueue buffers an event, flushing when the buffer reaches MaxBatchSize or
// scheduling a timer-based flush otherwise.
func (r *Recorder) Enqueue(event *RequestEvent) {
	if r.appID == "" || event == nil {
		return
	}

	r.mu.Lock()
	r.buffer = append(r.buffer, event)
	shouldFlush := len(r.buffer) >= r.opts.maxBatchSize()
	if !shouldFlush && r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(r.opts.flushInterval(), r.timerFlush)
	}
	r.mu.Unlock()

	if shouldFlush {
		r.Flush()
	}
}

func (r *Recorder) timerFlush() {
	r.mu.Lock()
	r.flushTimer = nil
	r.mu.Unlock()
	r.Flush()
}

// Flush queues buffered events for upload in the background and returns
// immediately. Use Shutdown to wait for uploads to finish.
func (r *Recorder) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.flushTimer != nil {
		r.flushTimer.Stop()
		r.flushTimer = nil
	}

	if r.appID == "" || len(r.buffer) == 0 {
		return
	}

	events := r.buffer
	r.buffer = nil

	r.pending = append(r.pending, &EventBatch{
		AppID:   r.appID,
		SDK:     SDKInfo{Name: SDKName, Version: SDKVersion},
		Release: r.opts.Release,
		Events:  events,
	})

	if !r.working {
		r.working = true
		go r.uploadLoop()
	}
}

// Shutdown flushes buffered events and waits for all pending uploads to
// finish, or for ctx to be done, whichever comes first. Call it before your
// process exits (e.g., with defer in main) so the final batch is not lost.
func (r *Recorder) Shutdown(ctx context.Context) error {
	r.Flush()

	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	})
	defer stop()

	r.mu.Lock()
	defer r.mu.Unlock()
	for r.working && ctx.Err() == nil {
		r.cond.Wait()
	}
	return ctx.Err()
}

func (r *Recorder) uploadLoop() {
	for {
		r.mu.Lock()
		if len(r.pending) == 0 {
			r.working = false
			r.cond.Broadcast()
			r.mu.Unlock()
			return
		}
		batch := r.pending[0]
		r.pending = r.pending[1:]
		r.mu.Unlock()

		r.upload(batch)
	}
}

func (r *Recorder) upload(batch *EventBatch) {
	serverURL := strings.TrimSuffix(r.opts.ServerURL, "/")
	if serverURL == "" {
		serverURL = defaultServerURL
	}
	url := serverURL + MakeIngestPath(batch.AppID)

	body, err := json.Marshal(batch)
	if err != nil {
		r.reportError(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.opts.uploadTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		r.reportError(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "token "+r.opts.APIKey)
	req.Header.Set("User-Agent", SDKName+"/"+SDKVersion)

	resp, err := r.opts.httpClient().Do(req)
	if err != nil {
		r.reportError(err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.reportError(fmt.Errorf("LogRocket MCP ingest responded with status %d", resp.StatusCode))
	}
}
