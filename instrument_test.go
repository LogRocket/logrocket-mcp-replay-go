package mcpreplay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRecordsToolsResourcesAndErrors(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.GetUser = func(context.Context, mcp.Request) *RecordedUser { return &RecordedUser{ID: "user-1"} }

	recorder := Instrument(server, opts)
	if Instrument(server, opts) != recorder {
		t.Fatal("instrumenting twice should return the same recorder")
	}

	ctx := context.Background()
	session := connect(t, server)

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 2, "b": 3}})
	if err != nil || result.IsError {
		t.Fatalf("add: %v %+v", err, result)
	}
	if _, err := session.ListTools(ctx, nil); err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if _, err := session.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memo://note"}); err != nil {
		t.Fatalf("read resource: %v", err)
	}
	failed, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "explode", Arguments: map[string]any{}})
	if err != nil || !failed.IsError {
		t.Fatalf("explode: %v %+v", err, failed)
	}
	if _, err := session.ReadResource(ctx, &mcp.ReadResourceParams{URI: "memo://missing"}); err == nil {
		t.Fatal("expected missing resource error")
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "raw_fail", Arguments: map[string]any{}}); err == nil {
		t.Fatal("expected raw tool error")
	}

	shutdown(t, recorder)
	events := ingest.events()

	counts := map[string]int{}
	for _, event := range events {
		counts[event.Method]++
		if strings.HasPrefix(event.Method, "notifications/") {
			t.Errorf("recorded a notification: %s", event.Method)
		}
		if event.Type != "mcp.request" || event.StartTime <= 0 || event.DurationMs < 0 || event.Seq <= 0 {
			t.Errorf("malformed event: %s", mustJSON(t, event))
		}
	}
	if counts["tools/list"] != 1 || counts["tools/call"] != 3 || counts["resources/read"] != 2 {
		t.Errorf("method counts = %v", counts)
	}

	add := findEvent(t, events, byTarget("add"))
	if got := mustJSON(t, add.Params); got != `{"arguments":{"a":2,"b":3},"name":"add"}` {
		t.Errorf("add params = %s", got)
	}
	if add.Result == nil || add.Result.IsError {
		t.Fatalf("add result = %+v", add.Result)
	}
	if value := mustJSON(t, add.Result.Value); !strings.Contains(value, `"structuredContent":{"sum":5}`) {
		t.Errorf("add result value = %s", value)
	}
	if add.User == nil || add.User.ID != "user-1" {
		t.Errorf("add user = %+v", add.User)
	}
	if add.Client == nil || add.Client.Name != "Test Client" || add.Client.Version != "1.2.3" {
		t.Errorf("add client = %+v", add.Client)
	}
	if add.ProtocolVersion == "" {
		t.Error("add protocolVersion missing")
	}
	if add.InstanceID != recorder.InstanceID() {
		t.Errorf("add instanceID = %q, want %q", add.InstanceID, recorder.InstanceID())
	}

	list := findEvent(t, events, byMethod("tools/list"))
	if list.Target != "" || list.Params != nil {
		t.Errorf("tools/list event = %s", mustJSON(t, list))
	}

	explode := findEvent(t, events, byTarget("explode"))
	if explode.Result == nil || !explode.Result.IsError || explode.ErrorMessage != "" {
		t.Errorf("explode event = %s", mustJSON(t, explode))
	}

	missing := findEvent(t, events, byTarget("memo://missing"))
	if missing.ErrorName != "jsonrpc2.WireError" || missing.ErrorCode == 0 || !strings.Contains(missing.ErrorMessage, "not found") || missing.Result != nil {
		t.Errorf("missing event = %s", mustJSON(t, missing))
	}

	rawFail := findEvent(t, events, byTarget("raw_fail"))
	if rawFail.ErrorName != "errors.errorString" || rawFail.ErrorCode != 0 || rawFail.ErrorMessage != "io: read/write on closed pipe" || rawFail.Result != nil {
		t.Errorf("raw_fail event = %s", mustJSON(t, rawFail))
	}

	if batch := ingest.batches()[0].batch; batch.AppID != "acme/my-app" || batch.Release != "" {
		t.Errorf("batch = %+v", batch)
	}
}

func TestSanitizeAndCaptureFlags(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.Release = "9.9.9"
	opts.DisableResultCapture = true
	opts.SanitizeEvent = func(event *RequestEvent) *RequestEvent {
		if event.Method == "initialize" || event.Method == "server/discover" {
			return nil
		}
		return event
	}
	recorder := Instrument(server, opts)

	session := connect(t, server)
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 1}}); err != nil {
		t.Fatal(err)
	}
	shutdown(t, recorder)

	events := ingest.events()
	for _, event := range events {
		if event.Method == "initialize" || event.Method == "server/discover" {
			t.Errorf("sanitized event was recorded: %s", event.Method)
		}
	}
	call := findEvent(t, events, byMethod("tools/call"))
	if got := mustJSON(t, call.Result); got != `{}` {
		t.Errorf("result = %s, want {}", got)
	}
	if call.Params == nil {
		t.Error("params should still be captured")
	}
	if ingest.batches()[0].batch.Release != "9.9.9" {
		t.Errorf("release = %q", ingest.batches()[0].batch.Release)
	}
}

func TestDisableParamCapture(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.DisableParamCapture = true
	recorder := Instrument(server, opts)

	session := connect(t, server)
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 1}}); err != nil {
		t.Fatal(err)
	}
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.Params != nil || call.Target != "add" {
		t.Errorf("event = %s", mustJSON(t, call))
	}
}

func TestClampsOversizedClientControlledStrings(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	server.AddTool(&mcp.Tool{Name: "long_error", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("API returned 500: " + strings.Repeat("x", 20000))
	})
	recorder := Instrument(server, ingest.options(t))

	ctx := context.Background()
	session := connect(t, server)
	_, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "long_error", Arguments: map[string]any{}})
	_, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: strings.Repeat("t", 5000), Arguments: map[string]any{}})
	shutdown(t, recorder)

	events := ingest.events()
	longError := findEvent(t, events, byTarget("long_error"))
	if len(longError.ErrorMessage) != maxErrorMessageLength {
		t.Errorf("errorMessage length = %d", len(longError.ErrorMessage))
	}
	longTarget := findEvent(t, events, func(e *RequestEvent) bool { return strings.HasPrefix(e.Target, "ttt") })
	if len(longTarget.Target) != maxTargetLength {
		t.Errorf("target length = %d", len(longTarget.Target))
	}
}

func TestTruncatesOversizedCapturedValues(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.MaxValueLength = 20
	recorder := Instrument(server, opts)

	session := connect(t, server)
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 1}}); err != nil {
		t.Fatal(err)
	}
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if !strings.Contains(mustJSON(t, call.Params), TruncatedMarker) {
		t.Errorf("params = %s", mustJSON(t, call.Params))
	}
}

func TestGetUserPanicIsContained(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	var errs []error
	opts := ingest.options(t)
	opts.OnError = func(err error) { errs = append(errs, err) }
	opts.GetUser = func(context.Context, mcp.Request) *RecordedUser { panic("boom") }
	recorder := Instrument(server, opts)

	session := connect(t, server)
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 1}}); err != nil {
		t.Fatal(err)
	}
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.User != nil {
		t.Errorf("user = %+v", call.User)
	}
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "GetUser panicked") {
		t.Errorf("errors = %v", errs)
	}
}

func TestGetUserEmptyIDIsDropped(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	var errs []error
	opts := ingest.options(t)
	opts.OnError = func(err error) { errs = append(errs, err) }
	opts.GetUser = func(context.Context, mcp.Request) *RecordedUser { return &RecordedUser{Email: "a@example.com"} }
	recorder := Instrument(server, opts)

	session := connect(t, server)
	for range 2 {
		if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 1}}); err != nil {
			t.Fatal(err)
		}
	}
	shutdown(t, recorder)

	for _, event := range ingest.events() {
		if event.User != nil {
			t.Errorf("%s: user = %+v, want nil", event.Method, event.User)
		}
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "empty ID") {
		t.Errorf("errors = %v, want exactly one empty-ID error", errs)
	}
}

type headerRoundTripper struct {
	headers map[string]string
}

func (rt headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	for name, value := range rt.headers {
		req.Header.Set(name, value)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func connectHTTP(t *testing.T, endpoint string, headers map[string]string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "HTTP Client", Version: "2.0.0"}, nil)
	transport := &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Transport: headerRoundTripper{headers: headers}},
	}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("http client connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func serveHTTP(t *testing.T, server *mcp.Server, wrap func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	var handler http.Handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	if wrap != nil {
		handler = wrap(handler)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func callAdd(t *testing.T, session *mcp.ClientSession) {
	t.Helper()
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "add", Arguments: map[string]any{"a": 1, "b": 2}}); err != nil {
		t.Fatalf("add: %v", err)
	}
}

func TestStreamableHTTPCapturesRequestContext(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.RedactHeaders = []string{"X-Internal-Secret"}
	recorder := Instrument(server, opts)
	ts := serveHTTP(t, server, HTTPMiddleware)

	session := connectHTTP(t, ts.URL+"/mcp", map[string]string{
		"User-Agent":        "claude-code/1.2.3 (external, cli)",
		"X-Forwarded-For":   "203.0.113.7, 198.51.100.1",
		"Authorization":     "Bearer super-secret-token",
		"Cookie":            "session=abc",
		"X-Internal-Secret": "hunter2",
	})
	callAdd(t, session)
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.UserAgent != "claude-code/1.2.3 (external, cli)" {
		t.Errorf("userAgent = %q", call.UserAgent)
	}
	if call.ServerURL != ts.URL+"/mcp" {
		t.Errorf("serverUrl = %q, want %q", call.ServerURL, ts.URL+"/mcp")
	}
	if call.IPAddress != "203.0.113.7" {
		t.Errorf("ipAddress = %q", call.IPAddress)
	}
	if call.Client == nil || call.Client.Name != "HTTP Client" {
		t.Errorf("client = %+v", call.Client)
	}

	headers := call.Headers
	if headers["content-type"] != "application/json" {
		t.Errorf("content-type = %q", headers["content-type"])
	}
	for _, name := range []string{"authorization", "cookie", "x-internal-secret"} {
		if headers[name] != redactedValue {
			t.Errorf("%s = %q, want redacted", name, headers[name])
		}
	}
	if _, leaked := headers[strings.ToLower(httpContextHeader)]; leaked {
		t.Error("internal context header was captured")
	}

	sum := sha256.Sum256([]byte("super-secret-token"))
	if call.AuthHash != hex.EncodeToString(sum[:]) {
		t.Errorf("authHash = %q", call.AuthHash)
	}
	if strings.Contains(mustJSON(t, call), "super-secret-token") {
		t.Error("bearer token leaked into the event")
	}
}

func TestStreamableHTTPFallsBackToPeerAddress(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.DisableHeaderCapture = true
	recorder := Instrument(server, opts)
	ts := serveHTTP(t, server, HTTPMiddleware)

	callAdd(t, connectHTTP(t, ts.URL+"/mcp", nil))
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.IPAddress != "127.0.0.1" {
		t.Errorf("ipAddress = %q", call.IPAddress)
	}
	if call.Headers != nil {
		t.Errorf("headers = %v, want none", call.Headers)
	}
	if call.UserAgent == "" || call.ServerURL != ts.URL+"/mcp" {
		t.Errorf("userAgent = %q serverUrl = %q", call.UserAgent, call.ServerURL)
	}
	if call.AuthHash != "" || call.OAuthClientID != "" {
		t.Errorf("unauthenticated request has authHash=%q oauthClientID=%q", call.AuthHash, call.OAuthClientID)
	}
}

func TestWithoutHTTPMiddlewareIgnoresForgedContext(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	recorder := Instrument(server, ingest.options(t))
	ts := serveHTTP(t, server, nil)

	callAdd(t, connectHTTP(t, ts.URL+"/mcp", map[string]string{
		httpContextHeader: "forged 198.51.100.99 https://evil.example/mcp",
	}))
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.ServerURL != "" || call.IPAddress != "" {
		t.Errorf("forged context accepted: serverUrl=%q ipAddress=%q", call.ServerURL, call.IPAddress)
	}
	if call.Headers["content-type"] == "" {
		t.Error("headers should still be captured without HTTPMiddleware")
	}
}

func TestBearerTokenInfo(t *testing.T) {
	ingest := startIngest(t)
	server := newDemoServer()
	opts := ingest.options(t)
	opts.GetUser = func(_ context.Context, req mcp.Request) *RecordedUser {
		if extra := req.GetExtra(); extra != nil && extra.TokenInfo != nil {
			return &RecordedUser{ID: extra.TokenInfo.UserID}
		}
		return nil
	}
	recorder := Instrument(server, opts)

	verifier := func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if token != "valid-token" {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{
			Expiration: time.Now().Add(time.Hour),
			UserID:     "user-42",
			Extra:      map[string]any{"client_id": "oauth-client-1"},
		}, nil
	}
	ts := serveHTTP(t, server, func(next http.Handler) http.Handler {
		return auth.RequireBearerToken(verifier, nil)(HTTPMiddleware(next))
	})

	callAdd(t, connectHTTP(t, ts.URL+"/mcp", map[string]string{"Authorization": "Bearer valid-token"}))
	shutdown(t, recorder)

	call := findEvent(t, ingest.events(), byMethod("tools/call"))
	if call.User == nil || call.User.ID != "user-42" {
		t.Errorf("user = %+v", call.User)
	}
	if call.OAuthClientID != "oauth-client-1" {
		t.Errorf("oauthClientID = %q", call.OAuthClientID)
	}
	sum := sha256.Sum256([]byte("valid-token"))
	if call.AuthHash != hex.EncodeToString(sum[:]) {
		t.Errorf("authHash = %q", call.AuthHash)
	}
}
