package mcpreplay

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// TruncatedMarker is appended to captured values that exceeded MaxValueLength.
const TruncatedMarker = "[truncated by " + SDKName + "]"

// ClampString returns value cut to at most maxLength bytes.
func ClampString(value string, maxLength int) string {
	if len(value) <= maxLength {
		return value
	}
	return value[:maxLength]
}

// TruncateValue returns value unchanged if its JSON serialization fits in
// maxLength bytes, and a truncated string representation otherwise.
func TruncateValue(value any, maxLength int) any {
	if value == nil {
		return nil
	}

	serialized, err := json.Marshal(value)
	if err != nil {
		return TruncatedMarker
	}

	if len(serialized) <= maxLength {
		return value
	}

	if s, ok := value.(string); ok {
		return ClampString(s, maxLength) + "... " + TruncatedMarker
	}

	return string(serialized[:maxLength]) + "... " + TruncatedMarker
}

// prepare applies capture, redaction, and sanitization options to an event
// and drops it (returning nil) if ingest would reject it.
func (r *Recorder) prepare(event *RequestEvent, opts Options) (prepared *RequestEvent) {
	if event == nil {
		return nil
	}

	defer func() {
		if p := recover(); p != nil {
			prepared = nil
			r.reportError(fmt.Errorf("preparing %s event panicked: %v", event.Method, p))
		}
	}()

	event.Type = "mcp.request"
	event.InstanceID = r.instanceID
	event.Seq = r.NextSeq()
	event.DurationMs = max(0, event.DurationMs)

	if opts.DisableParamCapture {
		event.Params = nil
	} else {
		event.Params = TruncateValue(event.Params, opts.maxValueLength())
	}

	if event.Result != nil {
		if opts.DisableResultCapture {
			event.Result.Value = nil
		} else {
			event.Result.Value = TruncateValue(event.Result.Value, opts.maxValueLength())
		}
	}

	if opts.DisableHeaderCapture {
		event.Headers = nil
	} else {
		event.Headers = sanitizeHeaders(event.Headers, opts)
	}

	if opts.SanitizeEvent != nil {
		if event = opts.SanitizeEvent(event); event == nil {
			return nil
		}
	}

	// Ingest rejects the whole batch for a user without an ID, so drop the
	// user and let the request sessionize as unidentified instead.
	if event.User != nil && event.User.ID == "" {
		r.emptyUserIDOnce.Do(func() {
			r.reportError(errors.New("RecordedUser has an empty ID; ignoring user"))
		})
		event.User = nil
	}

	if event.Method == "" || event.StartTime <= 0 {
		r.reportError(errors.New("dropping event without a Method or StartTime"))
		return nil
	}

	return event
}

func sanitizeHeaders(headers map[string]string, opts Options) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	redacted := make(map[string]bool, len(opts.RedactHeaders))
	for _, name := range opts.RedactHeaders {
		redacted[strings.ToLower(name)] = true
	}

	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	slices.SortFunc(names, func(a, b string) int { return strings.Compare(strings.ToLower(a), strings.ToLower(b)) })

	sanitized := make(map[string]string, min(len(names), maxCapturedHeaders))
	for _, name := range names[:min(len(names), maxCapturedHeaders)] {
		lowered := strings.ToLower(name)
		value := headers[name]
		if alwaysRedactedHeaders[lowered] || redacted[lowered] {
			value = redactedValue
		} else {
			value = ClampString(value, maxHeaderValueLength)
		}
		sanitized[ClampString(lowered, maxHeaderNameLength)] = value
	}
	return sanitized
}
