package mcpreplay

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"weak"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	redactedValue      = "[REDACTED]"
	maxCapturedHeaders = 64

	maxMethodLength          = 256
	maxTargetLength          = 2048
	maxErrorMessageLength    = 10000
	maxErrorNameLength       = 256
	maxClientNameLength      = 256
	maxClientVersionLength   = 256
	maxUserAgentLength       = 2048
	maxHeaderNameLength      = 256
	maxHeaderValueLength     = 2048
	maxServerURLLength       = 2048
	maxIPAddressLength       = 64
	maxProtocolVersionLength = 64
	maxTraceparentLength     = 512
	maxTracestateLength      = 1024
	maxBaggageLength         = 8192
	maxOAuthClientIDLength   = 512
	maxSessionIDLength       = 256
)

var alwaysRedactedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
	"api-key":             true,
	"x-auth-token":        true,
}

// The Go SDK hands middleware only the HTTP headers of a request, so
// HTTPMiddleware smuggles the request URL and peer address through a header.
// The nonce stops clients from forging that header themselves.
const httpContextHeader = "X-Logrocket-Mcp-Replay-Request"

var httpContextNonce = rand.Text()

var (
	registryMu      sync.Mutex
	instrumented    = map[weak.Pointer[mcp.Server]]*Recorder{}
	sharedRecorders = map[string]*Recorder{}
)

// Instrument records every request the server handles (tool calls, resource
// reads, prompt gets, listings, etc.) to LogRocket, including handlers
// registered afterwards. Instrumenting the same server twice is a no-op.
//
// The returned Recorder is the one events are buffered in; call its Shutdown
// before the process exits so the final batch is uploaded.
func Instrument(server *mcp.Server, opts Options) *Recorder {
	registryMu.Lock()
	defer registryMu.Unlock()

	key := weak.Make(server)
	if recorder, ok := instrumented[key]; ok {
		return recorder
	}

	recorder := resolveRecorder(opts)
	instrumented[key] = recorder
	runtime.AddCleanup(server, func(key weak.Pointer[mcp.Server]) {
		registryMu.Lock()
		delete(instrumented, key)
		registryMu.Unlock()
	}, key)

	server.AddReceivingMiddleware(NewMiddleware(recorder, opts))
	return recorder
}

func resolveRecorder(opts Options) *Recorder {
	if opts.Recorder != nil {
		return opts.Recorder
	}

	key := opts.APIKey + "|" + opts.ServerURL
	recorder, ok := sharedRecorders[key]
	if !ok {
		recorder = NewRecorder(opts)
		sharedRecorders[key] = recorder
	}
	return recorder
}

// NewMiddleware returns receiving middleware that records every request to
// the given recorder. Instrument installs it for you; use it directly with
// server.AddReceivingMiddleware to control ordering relative to your own
// middleware.
func NewMiddleware(recorder *Recorder, opts Options) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if strings.HasPrefix(method, "notifications/") {
				return next(ctx, method, req)
			}

			start := time.Now()
			result, err := next(ctx, method, req)
			record(ctx, recorder, opts, method, req, start, result, err)
			return result, err
		}
	}
}

// HTTPMiddleware wraps the HTTP handler serving your MCP server (e.g., an
// *mcp.StreamableHTTPHandler) so recorded requests include the server URL and
// the client's peer address. Without it, the client IP is available only from
// proxy headers (X-Forwarded-For and similar) and the server URL is omitted.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.Header.Set(httpContextHeader, httpContextNonce+" "+peerHost(req)+" "+requestURL(req))
		next.ServeHTTP(w, req)
	})
}

func peerHost(req *http.Request) string {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	if host == "" {
		return "-"
	}
	return host
}

func requestURL(req *http.Request) string {
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	if proto, _, _ := strings.Cut(req.Header.Get("X-Forwarded-Proto"), ","); strings.TrimSpace(proto) != "" {
		scheme = strings.TrimSpace(proto)
	}
	return scheme + "://" + req.Host + req.URL.EscapedPath()
}

func httpContext(header http.Header) (serverURL, peer string) {
	fields := strings.Fields(header.Get(httpContextHeader))
	if len(fields) != 3 || fields[0] != httpContextNonce {
		return "", ""
	}
	if fields[1] != "-" {
		peer = fields[1]
	}
	return fields[2], peer
}

func record(
	ctx context.Context,
	recorder *Recorder,
	opts Options,
	method string,
	req mcp.Request,
	start time.Time,
	result mcp.Result,
	err error,
) {
	defer func() {
		if p := recover(); p != nil {
			recorder.reportError(fmt.Errorf("recording %s panicked: %v", method, p))
		}
	}()

	if event := recorder.prepare(buildEvent(ctx, recorder, opts, method, req, start, result, err), opts); event != nil {
		recorder.enqueue(event)
	}
}

type requestInfo interface {
	ClientInfo() *mcp.Implementation
	ProtocolVersion() string
}

func buildEvent(
	ctx context.Context,
	recorder *Recorder,
	opts Options,
	method string,
	req mcp.Request,
	start time.Time,
	result mcp.Result,
	err error,
) *RequestEvent {
	params, _ := decodeJSON(req.GetParams()).(map[string]any)
	meta, _ := params["_meta"].(map[string]any)
	method = ClampString(method, maxMethodLength)

	event := &RequestEvent{
		StartTime:  start.UnixMilli(),
		DurationMs: time.Since(start).Milliseconds(),
		Method:     method,
		Target:     extractTarget(method, params),
		Client:     extractClient(req),
	}

	if opts.GetUser != nil {
		event.User = resolveUser(ctx, req, recorder, opts)
	}
	if opts.GetSessionID != nil {
		event.SessionID = resolveSessionID(ctx, req, recorder, opts)
	}

	extractHTTPInfo(event, req.GetExtra(), opts)

	if info, ok := req.(requestInfo); ok {
		event.ProtocolVersion = ClampString(info.ProtocolVersion(), maxProtocolVersionLength)
	}
	event.Traceparent = metaString(meta, "traceparent", maxTraceparentLength)
	event.Tracestate = metaString(meta, "tracestate", maxTracestateLength)
	event.Baggage = metaString(meta, "baggage", maxBaggageLength)

	if extra := req.GetExtra(); extra != nil {
		event.OAuthClientID = oauthClientID(extra.TokenInfo)
		event.AuthHash = hashBearerToken(extra.Header)
	}

	delete(params, "_meta")
	if len(params) > 0 {
		event.Params = params
	}

	if err != nil {
		event.ErrorMessage = ClampString(err.Error(), maxErrorMessageLength)
		event.ErrorName = ClampString(strings.TrimPrefix(fmt.Sprintf("%T", err), "*"), maxErrorNameLength)
		var wire *jsonrpc.Error
		if errors.As(err, &wire) {
			event.ErrorCode = wire.Code
		}
	} else if result != nil {
		value := decodeJSON(result)
		recorded := &RecordedResult{Value: value}
		if m, ok := value.(map[string]any); ok && m["isError"] == true {
			recorded.IsError = true
		}
		event.Result = recorded
	}

	return event
}

func resolveUser(ctx context.Context, req mcp.Request, recorder *Recorder, opts Options) (user *RecordedUser) {
	defer func() {
		if p := recover(); p != nil {
			user = nil
			recorder.reportError(fmt.Errorf("GetUser panicked: %v", p))
		}
	}()
	return opts.GetUser(ctx, req)
}

func resolveSessionID(ctx context.Context, req mcp.Request, recorder *Recorder, opts Options) (sessionID string) {
	defer func() {
		if p := recover(); p != nil {
			sessionID = ""
			recorder.reportError(fmt.Errorf("GetSessionID panicked: %v", p))
		}
	}()
	return ClampString(opts.GetSessionID(ctx, req), maxSessionIDLength)
}

func decodeJSON(value any) any {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var decoded any
	if json.Unmarshal(raw, &decoded) != nil {
		return nil
	}
	return decoded
}

func extractTarget(method string, params map[string]any) string {
	var key string
	switch method {
	case "tools/call", "prompts/get":
		key = "name"
	case "resources/read", "resources/subscribe", "resources/unsubscribe":
		key = "uri"
	default:
		return ""
	}
	target, _ := params[key].(string)
	return ClampString(target, maxTargetLength)
}

func extractClient(req mcp.Request) *RecordedClient {
	info, ok := req.(requestInfo)
	if !ok {
		return nil
	}
	impl := info.ClientInfo()
	if impl == nil || (impl.Name == "" && impl.Version == "") {
		return nil
	}
	return &RecordedClient{
		Name:    ClampString(impl.Name, maxClientNameLength),
		Version: ClampString(impl.Version, maxClientVersionLength),
	}
}

func metaString(meta map[string]any, key string, maxLength int) string {
	value, _ := meta[key].(string)
	return ClampString(value, maxLength)
}

func extractHTTPInfo(event *RequestEvent, extra *mcp.RequestExtra, opts Options) {
	if extra == nil || extra.Header == nil {
		return
	}
	header := extra.Header

	serverURL, peer := httpContext(header)
	event.ServerURL = ClampString(serverURL, maxServerURLLength)
	event.UserAgent = ClampString(header.Get("User-Agent"), maxUserAgentLength)

	forwardedFor, _, _ := strings.Cut(header.Get("X-Forwarded-For"), ",")
	for _, candidate := range []string{header.Get("Cf-Connecting-Ip"), strings.TrimSpace(forwardedFor), header.Get("X-Real-Ip"), peer} {
		if candidate != "" {
			event.IPAddress = ClampString(candidate, maxIPAddressLength)
			break
		}
	}

	if !opts.DisableHeaderCapture {
		event.Headers = make(map[string]string, len(header))
		for name, values := range header {
			if !strings.EqualFold(name, httpContextHeader) {
				event.Headers[name] = strings.Join(values, ", ")
			}
		}
	}
}

// auth.TokenInfo has no client ID field; RFC 9068 access tokens carry it as
// "client_id" and OIDC tokens as "azp".
func oauthClientID(info *auth.TokenInfo) string {
	if info == nil {
		return ""
	}
	for _, key := range []string{"client_id", "azp"} {
		if value, ok := info.Extra[key].(string); ok && value != "" {
			return ClampString(value, maxOAuthClientIDLength)
		}
	}
	return ""
}

func hashBearerToken(header http.Header) string {
	value := header.Get("Authorization")
	if len(value) < 7 || !strings.EqualFold(value[:7], "Bearer ") {
		return ""
	}
	token := strings.TrimSpace(value[7:])
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
