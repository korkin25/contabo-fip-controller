package main

import (
	"strings"
	"testing"
)

func TestCredentialsFromEnv(t *testing.T) {
	t.Setenv("CONTABO_CLIENT_ID", "id")
	t.Setenv("CONTABO_CLIENT_SECRET", "secret")
	t.Setenv("CONTABO_API_USER", "")
	t.Setenv("CONTABO_API_PASSWORD", "pw")
	if _, err := credentialsFromEnv(); err == nil || !strings.Contains(err.Error(), "CONTABO_API_USER") {
		t.Fatalf("want missing CONTABO_API_USER, got %v", err)
	}
	t.Setenv("CONTABO_API_USER", "user")
	c, err := credentialsFromEnv()
	if err != nil || c.Username != "user" || c.Password != "pw" {
		t.Fatalf("got %+v, %v", c, err)
	}
}

func TestServiceList(t *testing.T) {
	var s serviceList
	if err := s.Set("envoy/public, netbird/relay"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("bad"); err == nil {
		t.Fatal("want error")
	}
	if s.String() != "envoy/public,netbird/relay" {
		t.Fatalf("got %q", s.String())
	}
}
