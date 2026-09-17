// Package mcpreplay records the requests an MCP server built on
// github.com/modelcontextprotocol/go-sdk handles as LogRocket sessions.
//
// Call [Instrument] on an *mcp.Server to record every request it handles,
// and [Recorder.Shutdown] before the process exits to upload the final batch.
// On HTTP transports, wrap the handler with [HTTPMiddleware] to also record
// the server URL and client peer address.
package mcpreplay
