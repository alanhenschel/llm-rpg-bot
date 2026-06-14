package kafka

import (
	"testing"
)

func TestDeterministicEventID_SameInputSameOutput(t *testing.T) {
	id1 := DeterministicEventID("sender@s.whatsapp.net", 1718294400000, "hello world")
	id2 := DeterministicEventID("sender@s.whatsapp.net", 1718294400000, "hello world")
	if id1 != id2 {
		t.Fatalf("expected deterministic id, got %q and %q", id1, id2)
	}
}

func TestDeterministicEventID_DifferentBodyDifferentID(t *testing.T) {
	id1 := DeterministicEventID("sender@s.whatsapp.net", 1718294400000, "hello")
	id2 := DeterministicEventID("sender@s.whatsapp.net", 1718294400000, "world")
	if id1 == id2 {
		t.Fatal("expected different ids for different bodies")
	}
}

func TestDeterministicEventID_DifferentSenderDifferentID(t *testing.T) {
	id1 := DeterministicEventID("alice@s.whatsapp.net", 1718294400000, "hi")
	id2 := DeterministicEventID("bob@s.whatsapp.net", 1718294400000, "hi")
	if id1 == id2 {
		t.Fatal("expected different ids for different senders")
	}
}

func TestDeterministicEventID_DifferentTimestampDifferentID(t *testing.T) {
	id1 := DeterministicEventID("sender@s.whatsapp.net", 1000, "hi")
	id2 := DeterministicEventID("sender@s.whatsapp.net", 2000, "hi")
	if id1 == id2 {
		t.Fatal("expected different ids for different timestamps")
	}
}

func TestDeterministicEventID_IsValidUUIDFormat(t *testing.T) {
	id := DeterministicEventID("sender@s.whatsapp.net", 1718294400000, "test")
	if len(id) != 36 {
		t.Fatalf("expected uuid format (36 chars), got %q (len %d)", id, len(id))
	}
}
