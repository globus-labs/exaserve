package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishReadyHandshake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.json")
	if err := publishReadyHandshake(path, "nonce"); err != nil {
		t.Fatalf("publishReadyHandshake: %v", err)
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	var payload struct {
		SchemaVersion int    `json:"schema_version"`
		PID           int    `json:"pid"`
		Token         string `json:"token"`
	}
	if err := json.Unmarshal(blob, &payload); err != nil {
		t.Fatalf("decode handshake: %v", err)
	}
	if payload.SchemaVersion != 1 || payload.PID != os.Getpid() || payload.Token != "nonce" {
		t.Fatalf("unexpected handshake: %+v", payload)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat handshake: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("handshake mode = %o, want 600", info.Mode().Perm())
	}
}

func TestPublishReadyHandshakeRequiresIdentity(t *testing.T) {
	if err := publishReadyHandshake("", ""); err == nil {
		t.Fatal("missing handshake identity was accepted")
	}
}
