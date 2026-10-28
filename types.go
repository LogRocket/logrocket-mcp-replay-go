package mcpreplay

import (
	"context"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RecordedUser identifies the LogRocket user a request belongs to.
type RecordedUser struct {
	ID        string            `json:"id"`
	Email     string            `json:"email,omitempty"`
	Name      string            `json:"name,omitempty"`
	Anonymous bool              `json:"anonymous,omitempty"`
	Traits    map[string]string `json:"traits,omitempty"`
}

// RecordedClient identifies the MCP client application that sent a request.
type RecordedClient struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// RecordedResult is the outcome of a request that returned a result.
type RecordedResult struct {
	// IsError is true when the handler returned an isError result (e.g., a failed tool call).
	IsError bool `json:"isError,omitempty"`
	// Value is the handler's result, truncated to MaxValueLength.
	Value any `json:"value,omitempty"`
}

// RequestEvent is one recorded request. Field names match the LogRocket
// ingest schema.
type RequestEvent struct {
	Type string `json:"type"`
	// StartTime is epoch milliseconds when handling of the request started.
	StartTime  int64 `json:"startTime"`
	DurationMs int64 `json:"durationMs"`
	// Method is the JSON-RPC method (e.g., "tools/call", "resources/read", "initialize").
	Method string `json:"method"`
	// Target is the tool/prompt name or resource URI the request addressed, when applicable.
	Target string `json:"target,omitempty"`
	// RequestID is the JSON-RPC request ID. The Go SDK does not expose it to
	// middleware, so it is left unset.
	RequestID any `json:"requestID,omitempty"`
	// Params holds the request params (minus _meta), truncated to MaxValueLength.
	Params any             `json:"params,omitempty"`
	Result *RecordedResult `json:"result,omitempty"`
	// ErrorMessage is set when the handler returned an error rather than a result.
	ErrorMessage string `json:"errorMessage,omitempty"`
	// ErrorName is the error's Go type (e.g., "jsonrpc2.WireError"), used for issue grouping.
	ErrorName string `json:"errorName,omitempty"`
	// ErrorCode is the JSON-RPC error code carried by the returned error
	// (e.g., -32602 invalid params). Distinguishes client mistakes from server bugs.
	ErrorCode int64           `json:"errorCode,omitempty"`
	User      *RecordedUser   `json:"user,omitempty"`
	Client    *RecordedClient `json:"client,omitempty"`
	// UserAgent is the raw User-Agent header from the MCP client's HTTP request.
	UserAgent string `json:"userAgent,omitempty"`
	// Headers are the HTTP request headers, with sensitive values redacted.
	Headers map[string]string `json:"headers,omitempty"`
	// ServerURL is the URL of the MCP server endpoint that handled the request,
	// without query string. Requires HTTPMiddleware.
	ServerURL string `json:"serverUrl,omitempty"`
	// IPAddress is the client IP from standard proxy headers (or the peer
	// address when HTTPMiddleware is used), used for geo enrichment.
	IPAddress       string `json:"ipAddress,omitempty"`
	ProtocolVersion string `json:"protocolVersion,omitempty"`
	Traceparent     string `json:"traceparent,omitempty"`
	Tracestate      string `json:"tracestate,omitempty"`
	Baggage         string `json:"baggage,omitempty"`
	// OAuthClientID is the OAuth client ID from the validated access token, when available.
	OAuthClientID string `json:"oauthClientID,omitempty"`
	// AuthHash is the SHA-256 hex digest of the bearer token, when the request
	// was authenticated. Used server-side to group requests into per-user
	// sessions when GetUser is not configured. The token itself is never sent.
	AuthHash string `json:"authHash,omitempty"`
	// InstanceID is a random ID generated once per recorder (i.e., per server
	// process). Last-resort sessionization key for transports with no HTTP
	// context where one process serves one user (e.g., stdio).
	InstanceID string `json:"instanceID,omitempty"`
	// SessionID is an app-defined ID (e.g., a conversation ID) that splits
	// requests from the same user and client into separate LogRocket
	// sessions. It never merges requests the identity signals above would
	// separate. Sessions still end after 30 minutes of inactivity.
	SessionID string `json:"sessionID,omitempty"`
	Seq       int64  `json:"seq"`
}

// SDKInfo identifies the SDK that produced a batch.
type SDKInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// EventBatch is the payload uploaded to the LogRocket ingest server.
type EventBatch struct {
	AppID   string          `json:"appID"`
	SDK     SDKInfo         `json:"sdk"`
	Release string          `json:"release,omitempty"`
	Events  []*RequestEvent `json:"events"`
}

// Options configure recording. The zero value of every field except APIKey
// selects the default.
type Options struct {
	// APIKey is a LogRocket API key ("pat:org:app:secret", or the legacy
	// "org:app:secret" format). Authenticates with the ingest server and
	// identifies the app recorded to.
	APIKey string
	// ServerURL is the ingest server origin. Defaults to the production LogRocket ingest server.
	ServerURL string
	// Release is your MCP server's release identifier (e.g., a version or git
	// SHA). Shown on sessions and used by LogRocket's release filtering and comparison.
	Release string
	// DisableParamCapture skips capturing request params.
	DisableParamCapture bool
	// DisableResultCapture skips capturing request results.
	DisableResultCapture bool
	// DisableHeaderCapture skips capturing HTTP request headers. When
	// captured, sensitive values are always redacted.
	DisableHeaderCapture bool
	// RedactHeaders lists header names (case-insensitive) to redact in
	// addition to the built-in denylist.
	RedactHeaders []string
	// MaxValueLength is the maximum JSON-serialized length for captured
	// params/results. Defaults to 100000.
	MaxValueLength int
	// SanitizeEvent transforms or drops (by returning nil) an event before it is buffered.
	SanitizeEvent func(event *RequestEvent) *RequestEvent
	// MaxBatchSize flushes the buffer whenever it reaches this many events. Defaults to 25.
	MaxBatchSize int
	// FlushInterval flushes the buffer on this interval. Defaults to 5s.
	FlushInterval time.Duration
	// UploadTimeout aborts an ingest upload after this long. Defaults to 10s.
	UploadTimeout time.Duration
	// HTTPClient is used for ingest uploads. Defaults to http.DefaultClient.
	HTTPClient *http.Client
	// OnError is called with upload/serialization errors. These never
	// propagate to the host server.
	OnError func(err error)

	// Recorder shares one recorder (and its upload batching) across
	// Instrument calls. Used by Instrument only.
	Recorder *Recorder
	// GetUser resolves the LogRocket user for a request. req.GetExtra()
	// carries the HTTP headers and validated bearer token info on HTTP
	// transports (nil on stdio). Used by Instrument only.
	GetUser func(ctx context.Context, req mcp.Request) *RecordedUser
	// GetSessionID resolves an app-defined session ID (e.g., a conversation
	// ID) for a request; see RequestEvent.SessionID. Return "" to fall back
	// to the default grouping. Used by Instrument only.
	GetSessionID func(ctx context.Context, req mcp.Request) string
}

const (
	defaultServerURL      = "https://api.logrocket.com"
	defaultMaxValueLength = 100000
	defaultMaxBatchSize   = 25
	defaultFlushInterval  = 5 * time.Second
	defaultUploadTimeout  = 10 * time.Second
)

func (o Options) maxValueLength() int {
	if o.MaxValueLength > 0 {
		return o.MaxValueLength
	}
	return defaultMaxValueLength
}

func (o Options) maxBatchSize() int {
	if o.MaxBatchSize > 0 {
		return o.MaxBatchSize
	}
	return defaultMaxBatchSize
}

func (o Options) flushInterval() time.Duration {
	if o.FlushInterval > 0 {
		return o.FlushInterval
	}
	return defaultFlushInterval
}

func (o Options) uploadTimeout() time.Duration {
	if o.UploadTimeout > 0 {
		return o.UploadTimeout
	}
	return defaultUploadTimeout
}

func (o Options) httpClient() *http.Client {
	if o.HTTPClient != nil {
		return o.HTTPClient
	}
	return http.DefaultClient
}
