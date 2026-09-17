# Changelog

## 0.1.0

- Initial release: `Instrument()` records requests handled by servers built on the official `github.com/modelcontextprotocol/go-sdk` (v1.x) via receiving middleware and uploads batched request events (tool calls, resource reads, prompt gets, listings, etc.) to LogRocket. `HTTPMiddleware` adds the server URL and peer address on HTTP transports.
