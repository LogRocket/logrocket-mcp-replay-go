package mcpreplay

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func makeEvent(seq int64) *RequestEvent {
	return &RequestEvent{Type: "mcp.request", StartTime: 1, Method: "tools/call", Seq: seq}
}

func TestParseAppIDAndIngestPath(t *testing.T) {
	cases := map[string]string{
		"pat:acme:my-app:secret": "acme/my-app",
		"acme:my-app:secret":     "acme/my-app",
		"garbage":                "",
		"pat::app:secret":        "",
	}
	for key, want := range cases {
		if got := ParseAppID(key); got != want {
			t.Errorf("ParseAppID(%q) = %q, want %q", key, got, want)
		}
	}
	if got := MakeIngestPath("acme/my-app"); got != "/v1/orgs/acme/apps/my-app/mcp/events" {
		t.Errorf("MakeIngestPath = %q", got)
	}
}

func TestClampAndTruncate(t *testing.T) {
	if got := ClampString("abcdef", 3); got != "abc" {
		t.Errorf("ClampString = %q", got)
	}
	if got := TruncateValue(nil, 5); got != nil {
		t.Errorf("TruncateValue(nil) = %v", got)
	}
	small := map[string]any{"a": 1}
	if got := TruncateValue(small, 100); mustJSON(t, got) != `{"a":1}` {
		t.Errorf("TruncateValue(small) = %v", got)
	}
	if got := TruncateValue(strings.Repeat("x", 10), 4); got != "xxxx... "+TruncatedMarker {
		t.Errorf("TruncateValue(string) = %v", got)
	}
	truncated, ok := TruncateValue(map[string]any{"a": strings.Repeat("y", 100)}, 10).(string)
	if !ok || !strings.HasSuffix(truncated, TruncatedMarker) {
		t.Errorf("TruncateValue(map) = %v", truncated)
	}
	if got := TruncateValue(make(chan int), 5); got != TruncatedMarker {
		t.Errorf("TruncateValue(unserializable) = %v", got)
	}
}

func TestUploadPostsBatchWithAuth(t *testing.T) {
	ingest := startIngest(t)
	recorder := NewRecorder(Options{
		APIKey:    testAPIKey,
		ServerURL: ingest.URL + "/",
		Release:   "1.2.3",
		OnError:   func(err error) { t.Errorf("unexpected error: %v", err) },
	})

	recorder.Enqueue(makeEvent(1))
	recorder.Enqueue(makeEvent(2))
	shutdown(t, recorder)

	batches := ingest.batches()
	if len(batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(batches))
	}
	got := batches[0]
	if got.path != "/v1/orgs/acme/apps/my-app/mcp/events" {
		t.Errorf("path = %q", got.path)
	}
	if got.headers.Get("Authorization") != "token "+testAPIKey {
		t.Errorf("Authorization = %q", got.headers.Get("Authorization"))
	}
	if got.headers.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", got.headers.Get("Content-Type"))
	}
	if !strings.HasPrefix(got.headers.Get("User-Agent"), SDKName+"/") {
		t.Errorf("User-Agent = %q", got.headers.Get("User-Agent"))
	}
	if got.batch.AppID != "acme/my-app" || got.batch.Release != "1.2.3" || got.batch.SDK.Name != SDKName || got.batch.SDK.Version == "" {
		t.Errorf("batch metadata = %+v", got.batch)
	}
	if len(got.batch.Events) != 2 || got.batch.Events[0].Seq != 1 || got.batch.Events[1].Seq != 2 {
		t.Errorf("events = %s", mustJSON(t, got.batch.Events))
	}
}

func TestOmitsReleaseWhenUnset(t *testing.T) {
	ingest := startIngest(t)
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL})
	recorder.Enqueue(makeEvent(1))
	shutdown(t, recorder)

	raw := mustJSON(t, ingest.batches()[0].batch)
	if strings.Contains(raw, "release") {
		t.Errorf("batch should omit release: %s", raw)
	}
}

func TestFlushesWhenBatchSizeReached(t *testing.T) {
	ingest := startIngest(t)
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL, MaxBatchSize: 2})

	recorder.Enqueue(makeEvent(1))
	if len(ingest.batches()) != 0 {
		t.Fatal("flushed before reaching batch size")
	}
	recorder.Enqueue(makeEvent(2))
	shutdown(t, recorder)

	if len(ingest.batches()) != 1 {
		t.Fatalf("got %d batches, want 1", len(ingest.batches()))
	}
}

func TestFlushesOnTimer(t *testing.T) {
	ingest := startIngest(t)
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL, FlushInterval: 50 * time.Millisecond})
	recorder.Enqueue(makeEvent(1))

	deadline := time.Now().Add(5 * time.Second)
	for len(ingest.batches()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(ingest.batches()) != 1 {
		t.Fatalf("got %d batches, want 1", len(ingest.batches()))
	}
}

func TestUploadsBatchesOneAtATimeInOrder(t *testing.T) {
	ingest := startIngest(t)
	ingest.delay[1] = 300 * time.Millisecond
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL, MaxBatchSize: 1})

	for seq := int64(1); seq <= 3; seq++ {
		recorder.Enqueue(makeEvent(seq))
	}
	shutdown(t, recorder)

	var order []int64
	for _, r := range ingest.batches() {
		order = append(order, r.batch.Events[0].Seq)
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("upload order = %v", order)
	}
}

func TestReportsHTTPErrors(t *testing.T) {
	ingest := startIngest(t)
	ingest.status = http.StatusInternalServerError
	var errs []error
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL, OnError: func(err error) { errs = append(errs, err) }})

	recorder.Enqueue(makeEvent(1))
	shutdown(t, recorder)

	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "500") {
		t.Errorf("errors = %v", errs)
	}
}

func TestReportsNetworkErrors(t *testing.T) {
	ingest := startIngest(t)
	ingest.Close()
	var errs []error
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL, OnError: func(err error) { errs = append(errs, err) }})

	recorder.Enqueue(makeEvent(1))
	shutdown(t, recorder)

	if len(errs) != 1 {
		t.Errorf("errors = %v", errs)
	}
}

func TestInvalidAPIKeyDisablesRecording(t *testing.T) {
	ingest := startIngest(t)
	var errs []error
	recorder := NewRecorder(Options{APIKey: "nope", ServerURL: ingest.URL, OnError: func(err error) { errs = append(errs, err) }})

	if recorder.AppID() != "" {
		t.Errorf("AppID = %q, want empty", recorder.AppID())
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "recording is disabled") {
		t.Errorf("errors = %v", errs)
	}

	recorder.Enqueue(makeEvent(1))
	shutdown(t, recorder)
	if len(ingest.batches()) != 0 {
		t.Error("uploaded despite invalid API key")
	}
}

func TestShutdownHonorsContextDeadline(t *testing.T) {
	ingest := startIngest(t)
	ingest.delay[1] = 500 * time.Millisecond
	recorder := NewRecorder(Options{APIKey: testAPIKey, ServerURL: ingest.URL})
	recorder.Enqueue(makeEvent(1))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := recorder.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Errorf("Shutdown error = %v, want DeadlineExceeded", err)
	}
}
