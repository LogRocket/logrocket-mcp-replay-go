package mcpreplay

import "encoding/json"

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
