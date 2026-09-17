# LogRocket MCP Replay

Records the requests your MCP server handles as LogRocket sessions so you can see how users' agents interact with your product: which tools they call and with what arguments, which resources and prompts they fetch, what they got back, and where errors happen.

Works with MCP servers built on the official [`github.com/modelcontextprotocol/go-sdk`](https://pkg.go.dev/github.com/modelcontextprotocol/go-sdk/mcp) (v1.x) over any transport (stdio, Streamable HTTP, SSE, in-memory).

## Installation

```bash
go get github.com/LogRocket/logrocket-mcp-replay-go
```

## Usage

```go
import (
	mcpreplay "github.com/LogRocket/logrocket-mcp-replay-go"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

server := buildMyMcpServer()

recorder := mcpreplay.Instrument(server, mcpreplay.Options{
	APIKey: os.Getenv("LOGROCKET_INGEST_KEY"),
	GetUser: func(ctx context.Context, req mcp.Request) *mcpreplay.RecordedUser {
		if extra := req.GetExtra(); extra != nil && extra.TokenInfo != nil {
			return &mcpreplay.RecordedUser{ID: extra.TokenInfo.UserID}
		}
		return nil
	},
})
defer recorder.Shutdown(context.Background())
```

`Instrument` installs receiving middleware that records every request the server handles - tool calls, resource reads, prompt gets, listings, and any tool or resource added afterwards - and uploads batched request events to LogRocket from a background goroutine. Errors returned by handlers will be surfaced as LogRocket issues.

Events are buffered and flushed when the buffer fills or on a timer. Go has no exit hook, so call `recorder.Shutdown(ctx)` before your process exits to upload the final batch. On platforms that freeze the process as soon as a response is sent (e.g., AWS Lambda), call it before returning from each invocation.

### API key

Create an API key for your app in the LogRocket dashboard and pass it as `APIKey`. The key both authenticates uploads and determines which LogRocket app sessions are recorded to. There is no separate appID to configure.

### HTTP transports

The Go SDK exposes only request headers to server middleware, so wrap your HTTP handler with `HTTPMiddleware` to also record the server URL and the client's peer address (used when no proxy header like `X-Forwarded-For` is present):

```go
handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
http.Handle("/mcp", mcpreplay.HTTPMiddleware(handler))
```

If you create a server per request, instrument each one; `Instrument` shares a single recorder per API key, or pass your own via `Recorder`.

### Example

[`examples/streamable`](examples/streamable) is a runnable server with tools that exercise each recorded outcome:

```bash
LOGROCKET_INGEST_KEY=pat:org:app:secret go run -C examples/streamable .
```

## Sessions and identity

LogRocket assigns requests to sessions server-side, emulating web visits: one person using one MCP client is one session, ended by 30 minutes of inactivity. Requests are grouped per client app (name and version) by the best available identity signal:

1. The identified user, when `GetUser` is provided.
2. A SHA-256 hash of the bearer token, for OAuth-protected servers. The token itself is never sent.
3. The client IP and user agent, for unauthenticated servers.
4. A random per-process ID, for transports with no HTTP request (e.g., stdio), where one server process serves one user.

Provide `GetUser` for the best results. It receives the request context and the `mcp.Request`; on HTTP transports `req.GetExtra()` carries the request headers and, when you use the SDK's `auth.RequireBearerToken`, the validated `TokenInfo`, so the user can be resolved from the OAuth subject, an API key owner, or whatever your auth middleware attached. `RecordedUser.ID` is required; a user returned without one is ignored (reported once via `OnError`) and the request falls through to the next signal.

W3C trace context (`traceparent`/`baggage` in `_meta`) is captured on every request and shown with the request's headers in the replay, so requests can be correlated with your tracing backend. It is not used for session grouping.

## Options

| Option                 | Description                                                         |
| ---------------------- | ------------------------------------------------------------------- |
| `APIKey` (required)    | LogRocket API key; identifies the app recorded to (see above)       |
| `ServerURL`            | Ingest server origin override                                       |
| `Release`              | Your server's release (e.g., version, git SHA) for release tracking |
| `GetUser`              | Resolve the LogRocket user from the request                         |
| `DisableParamCapture`  | Skip capturing request params                                       |
| `DisableResultCapture` | Skip capturing request results                                      |
| `DisableHeaderCapture` | Skip capturing HTTP request headers                                 |
| `RedactHeaders`        | Additional header names to redact                                   |
| `MaxValueLength`       | Max JSON-serialized length of captured values (default `100000`)    |
| `SanitizeEvent`        | Transform or drop (return `nil`) events before upload               |
| `Recorder`             | Share one recorder across `Instrument` calls                        |
| `MaxBatchSize`         | Flush when the buffer reaches this many events (default `25`)       |
| `FlushInterval`        | Timer-based flush interval (default `5s`)                           |
| `UploadTimeout`        | Abort an ingest upload after this long (default `10s`)              |
| `HTTPClient`           | `*http.Client` used for uploads (default `http.DefaultClient`)      |
| `OnError`              | Callback for upload/serialization errors                            |

## Sanitization

HTTP request headers are captured so they appear alongside each request in the Network view. Credential-bearing headers (`Authorization`, `Cookie`, `X-Api-Key`, and similar) are always replaced with `[REDACTED]`. This can't be disabled. Use `RedactHeaders` to redact additional headers, or `DisableHeaderCapture` to skip header capture entirely.

Params and results are truncated to `MaxValueLength` by default. To handle requests containing sensitive data, use `SanitizeEvent` to redact fields or drop events entirely:

```go
mcpreplay.Instrument(server, mcpreplay.Options{
	APIKey: os.Getenv("LOGROCKET_INGEST_KEY"),
	SanitizeEvent: func(event *mcpreplay.RequestEvent) *mcpreplay.RequestEvent {
		if event.Method == "tools/call" && event.Target == "create_payment" {
			event.Params = nil
		}
		return event
	},
})
```
