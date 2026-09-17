package mcpreplay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const testAPIKey = "pat:acme:my-app:secret"

type received struct {
	path    string
	headers http.Header
	batch   EventBatch
}

type ingestServer struct {
	*httptest.Server
	mu       sync.Mutex
	received []received
	status   int
	delay    map[int64]time.Duration
}

func startIngest(t *testing.T) *ingestServer {
	t.Helper()
	ingest := &ingestServer{status: http.StatusOK, delay: map[int64]time.Duration{}}
	ingest.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		var batch EventBatch
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("malformed batch: %v", err)
		}
		ingest.mu.Lock()
		delay := ingest.delay[batch.Events[0].Seq]
		status := ingest.status
		ingest.mu.Unlock()

		time.Sleep(delay)

		ingest.mu.Lock()
		ingest.received = append(ingest.received, received{path: req.URL.Path, headers: req.Header.Clone(), batch: batch})
		ingest.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(ingest.Close)
	return ingest
}

func (s *ingestServer) batches() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.received...)
}

func (s *ingestServer) events() []*RequestEvent {
	var events []*RequestEvent
	for _, r := range s.batches() {
		events = append(events, r.batch.Events...)
	}
	return events
}

func (s *ingestServer) options(t *testing.T) Options {
	t.Helper()
	return Options{
		APIKey:       testAPIKey,
		ServerURL:    s.URL,
		MaxBatchSize: 1000,
		OnError:      func(err error) { t.Errorf("unexpected recorder error: %v", err) },
	}
}

func shutdown(t *testing.T, recorder *Recorder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := recorder.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

type addInput struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addOutput struct {
	Sum int `json:"sum"`
}

func newDemoServer() *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "demo", Version: "0.0.1"}, nil)

	mcp.AddTool(server, &mcp.Tool{Name: "add"}, func(_ context.Context, _ *mcp.CallToolRequest, in addInput) (*mcp.CallToolResult, addOutput, error) {
		return nil, addOutput{Sum: in.A + in.B}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "explode"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return nil, nil, io.ErrUnexpectedEOF
	})
	server.AddTool(&mcp.Tool{Name: "raw_fail", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return nil, io.ErrClosedPipe
	})
	server.AddResource(&mcp.Resource{URI: "memo://note", Name: "note"}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: req.Params.URI, Text: "remember"}}}, nil
	})

	return server
}

func connect(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "Test Client", Version: "1.2.3"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}

func findEvent(t *testing.T, events []*RequestEvent, match func(*RequestEvent) bool) *RequestEvent {
	t.Helper()
	for _, event := range events {
		if match(event) {
			return event
		}
	}
	t.Fatalf("no matching event among %d events", len(events))
	return nil
}

func byTarget(target string) func(*RequestEvent) bool {
	return func(e *RequestEvent) bool { return e.Target == target }
}

func byMethod(method string) func(*RequestEvent) bool {
	return func(e *RequestEvent) bool { return e.Method == method }
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}
