package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/goccy/go-json"
)

const unexpectedErrorFormat = "unexpected error: %v"

func TestResolvePayloadFormat(t *testing.T) {
	t.Parallel()

	format, err := resolvePayloadFormat("", "payload.yaml")
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if format != payloadFormatYAML {
		t.Fatalf("expected yaml format, got %s", format)
	}

	format, err = resolvePayloadFormat("", "payload.json")
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if format != payloadFormatJSON {
		t.Fatalf("expected json format, got %s", format)
	}

	_, err = resolvePayloadFormat("xml", "")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestDecodePayloadJSON(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"hello":"world"}`)

	out, err := decodePayload(raw, payloadFormatJSON)
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if !json.Valid(out) {
		t.Fatal("expected valid json output")
	}

	_, err = decodePayload([]byte(`{invalid`), payloadFormatJSON)
	if err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestDecodePayloadYAML(t *testing.T) {
	t.Parallel()

	raw := []byte("hello: world\ncount: 2\n")

	out, err := decodePayload(raw, payloadFormatYAML)
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if !json.Valid(out) {
		t.Fatal("expected json output for yaml input")
	}
}

func TestDecodePayloadBase64(t *testing.T) {
	t.Parallel()

	out, err := decodePayloadBase64([]byte("aGVsbG8="))
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if string(out) != "hello" {
		t.Fatalf("unexpected base64 output: %s", string(out))
	}
}

func TestLoadPayloadFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.yml")

	const filePerm = 0o600

	err := os.WriteFile(path, []byte("foo: bar\n"), filePerm)
	if err != nil {
		t.Fatalf("write file: %v", err)
	}

	out, err := loadPayload(enqueueOptions{payloadFile: path})
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if !json.Valid(out) {
		t.Fatal("expected json output from yaml file")
	}
}

func TestParseRunAt(t *testing.T) {
	t.Parallel()

	_, err := parseRunAt("2020-01-01T00:00:00Z", time.Second)
	if err == nil {
		t.Fatal("expected error for conflicting run-at and delay")
	}

	value, err := parseRunAt("2020-01-01T00:00:00Z", 0)
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if value.IsZero() {
		t.Fatal("expected parsed time")
	}
}

func TestParseMetadataPairs(t *testing.T) {
	t.Parallel()

	meta, err := parseMetadataPairs([]string{"env=prod", "team=core"})
	if err != nil {
		t.Fatalf(unexpectedErrorFormat, err)
	}

	if meta["env"] != "prod" || meta["team"] != "core" {
		t.Fatalf("unexpected metadata: %#v", meta)
	}

	_, err = parseMetadataPairs([]string{"invalid"})
	if err == nil {
		t.Fatal("expected error for invalid metadata")
	}
}

func TestPayloadSourceCount(t *testing.T) {
	t.Parallel()

	if payloadSourceCount(enqueueOptions{}) != 0 {
		t.Fatal("expected zero sources")
	}

	opts := enqueueOptions{payload: "{}"}
	if payloadSourceCount(opts) != 1 {
		t.Fatal("expected one source")
	}
}
