# Changelog

## 0.2.0

- Add `Options.GetSessionID` and `RequestEvent.SessionID` to split sessions by an app-defined session ID (e.g., a conversation ID).
- `Recorder.Enqueue` now applies the recorder's capture, redaction, and `SanitizeEvent` options and sets `Type`, `InstanceID`, and `Seq`, so servers that don't use the go-sdk can record requests with it. Events without a `Method` or `StartTime` are dropped instead of failing the batch.
- Document serverless usage and that `Shutdown` is safe to call repeatedly.

## 0.1.0

- Initial release: `Instrument()` records requests handled by servers built on the official `github.com/modelcontextprotocol/go-sdk` (v1.x) via receiving middleware and uploads batched request events (tool calls, resource reads, prompt gets, listings, etc.) to LogRocket. `HTTPMiddleware` adds the server URL and peer address on HTTP transports.
