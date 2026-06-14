package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHeaderValue_ReturnsMatchingHeader(t *testing.T) {
	rec := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "trace_id", Value: []byte("abc-123")},
		},
	}
	got := headerValue(rec, "trace_id")
	if got != "abc-123" {
		t.Fatalf("expected abc-123, got %q", got)
	}
}

func TestHeaderValue_ReturnEmptyWhenMissing(t *testing.T) {
	rec := &kgo.Record{Headers: []kgo.RecordHeader{}}
	got := headerValue(rec, "trace_id")
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestHeaderValue_ReturnEmptyWhenNilHeaders(t *testing.T) {
	rec := &kgo.Record{}
	got := headerValue(rec, "trace_id")
	if got != "" {
		t.Fatalf("expected empty string for nil headers, got %q", got)
	}
}

func TestHeaderValue_FirstMatchWins(t *testing.T) {
	rec := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "trace_id", Value: []byte("first")},
			{Key: "trace_id", Value: []byte("second")},
		},
	}
	got := headerValue(rec, "trace_id")
	if got != "first" {
		t.Fatalf("expected first, got %q", got)
	}
}
