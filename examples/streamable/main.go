// Command streamable runs a small MCP server over Streamable HTTP with
// LogRocket recording enabled, for trying the SDK end to end.
//
//	LOGROCKET_INGEST_KEY=pat:org:app:secret go run -C examples/streamable .
//
// Then point an MCP client at http://127.0.0.1:8080/mcp.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mcpreplay "github.com/LogRocket/logrocket-mcp-replay-go"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type greetInput struct {
	Name string `json:"name" jsonschema:"who to greet"`
}

type greetOutput struct {
	Greeting string `json:"greeting"`
}

type failInput struct {
	Reason string `json:"reason,omitempty" jsonschema:"why the tool should fail"`
}

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	apiKey := os.Getenv("LOGROCKET_INGEST_KEY")
	if apiKey == "" {
		log.Fatal("LOGROCKET_INGEST_KEY is required")
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "mcp-replay-demo", Version: "0.1.0"}, nil)

	mcp.AddTool(server, &mcp.Tool{Name: "greet", Description: "Say hello"}, func(_ context.Context, _ *mcp.CallToolRequest, in greetInput) (*mcp.CallToolResult, greetOutput, error) {
		return nil, greetOutput{Greeting: "Hi " + in.Name}, nil
	})

	mcp.AddTool(server, &mcp.Tool{Name: "fail", Description: "Always returns an isError tool result"}, func(_ context.Context, _ *mcp.CallToolRequest, in failInput) (*mcp.CallToolResult, any, error) {
		if in.Reason == "" {
			in.Reason = "no reason given"
		}
		return nil, nil, errors.New("tool failed: " + in.Reason)
	})

	server.AddTool(&mcp.Tool{Name: "crash", Description: "Always returns a JSON-RPC error", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, errors.New("simulated handler crash")
	})

	server.AddResource(&mcp.Resource{URI: "memo://note", Name: "note", MIMEType: "text/plain"}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: req.Params.URI, MIMEType: "text/plain", Text: "remember to flush the recorder"}}}, nil
	})

	recorder := mcpreplay.Instrument(server, mcpreplay.Options{
		APIKey:    apiKey,
		ServerURL: os.Getenv("LOGROCKET_SERVER_URL"),
		Release:   os.Getenv("LOGROCKET_RELEASE"),
		GetUser: func(_ context.Context, req mcp.Request) *mcpreplay.RecordedUser {
			if extra := req.GetExtra(); extra != nil {
				if id := extra.Header.Get("X-User-Id"); id != "" {
					return &mcpreplay.RecordedUser{ID: id}
				}
			}
			return nil
		},
		OnError: func(err error) { log.Printf("logrocket: %v", err) },
	})

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpreplay.HTTPMiddleware(handler))

	httpServer := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		display := *addr
		if strings.HasPrefix(display, ":") {
			display = "127.0.0.1" + display
		}
		log.Printf("MCP server listening on http://%s/mcp", display)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	// Clients hold a long-lived SSE stream open, which http.Server.Shutdown
	// would wait on until its deadline. Closing sessions ends those streams.
	for session := range server.Sessions() {
		_ = session.Close()
	}
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelHTTP()
	if err := httpServer.Shutdown(httpCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	recorderCtx, cancelRecorder := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRecorder()
	if err := recorder.Shutdown(recorderCtx); err != nil {
		log.Printf("recorder shutdown: %v", err)
	}
	fmt.Println("bye")
}
