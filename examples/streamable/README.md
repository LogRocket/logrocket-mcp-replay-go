# Streamable HTTP example

A small MCP server with LogRocket recording enabled. It exposes three tools that exercise each recorded outcome (`greet` succeeds, `fail` returns an `isError` result, `crash` returns a JSON-RPC error) and one resource.

This directory is its own Go module, so run it from here (or with `go run -C examples/streamable .` from the repo root):

```bash
LOGROCKET_INGEST_KEY=pat:org:app:secret go run .
```

Optional: `LOGROCKET_SERVER_URL` to target a different ingest origin, `LOGROCKET_RELEASE` to tag sessions with a release, `-addr` to change the listen address (default `:8080`).

Connect any MCP client to `http://127.0.0.1:8080/mcp`. For example, for Cursor, add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "mcp-replay-demo": {
      "url": "http://127.0.0.1:8080/mcp"
    }
  }
}
```

Requests are attributed to a user when the client sends an `X-User-Id` header; otherwise sessions are grouped by client IP and user agent.
