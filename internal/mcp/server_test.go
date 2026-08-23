package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffplourde/canon"
)

func TestToolsList(t *testing.T) {
	srv := New(testStore(t))
	resp, ok := srv.Handle([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if !ok {
		t.Fatal("no response")
	}
	var decoded struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp, &decoded); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(decoded.Result.Tools))
	for _, tool := range decoded.Result.Tools {
		got = append(got, tool.Name)
	}
	want := "put_claim,resolve_conflict,retract_claim,search,scan_external_changes"
	if strings.Join(got, ",") != want {
		t.Fatalf("tools = %s, want %s", strings.Join(got, ","), want)
	}
}

func TestPutClaimAndSearchToolCalls(t *testing.T) {
	srv := New(testStore(t))
	put := callTool(t, srv, "put_claim", map[string]any{
		"entity":     "Canon",
		"key":        "status",
		"value":      "ratified",
		"provenance": "agent-a",
	})
	if put["status"] != canon.ResultInserted {
		t.Fatalf("put result = %+v", put)
	}
	search := callToolAny(t, srv, "search", map[string]any{"query": "ratified"})
	if !strings.Contains(mustString(t, search), "ratified") {
		t.Fatalf("search result missing claim: %+v", search)
	}
}

func TestConflictingPutReturnsToolErrorFalseWithConflictPayload(t *testing.T) {
	srv := New(testStore(t))
	_ = callTool(t, srv, "put_claim", map[string]any{
		"entity":     "Canon",
		"key":        "license",
		"value":      "Apache-2.0",
		"provenance": "agent-a",
	})
	out := callTool(t, srv, "put_claim", map[string]any{
		"entity":     "Canon",
		"key":        "license",
		"value":      "AGPL-3.0",
		"provenance": "agent-b",
	})
	if out["status"] != canon.ResultConflicted || out["conflict"] == nil {
		t.Fatalf("conflict output = %+v", out)
	}
}

func TestServeUsesMCPFraming(t *testing.T) {
	srv := New(testStore(t))
	req := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	var out bytes.Buffer
	if err := srv.Serve(bytes.NewReader(EncodeForTest(req)), &out); err != nil {
		t.Fatal(err)
	}
	msg, err := readMessage(bufio.NewReader(&out))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), `"protocolVersion"`) {
		t.Fatalf("initialize response = %s", msg)
	}
}

func TestScanExternalChangesToolImportsEditedClaimBlock(t *testing.T) {
	store := testStore(t)
	first, err := store.PutClaim(canon.ClaimInput{Entity: "Canon", Key: "status", Value: "ratified", Provenance: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	note := strings.Replace(readFile(t, store, "canon.md"), "ratified", "debating", 1)
	writeFile(t, store, "canon.md", note)

	srv := New(store)
	out := callTool(t, srv, "scan_external_changes", map[string]any{"provenance": "human-edit"})
	if out["imported"].(float64) != 1 {
		t.Fatalf("scan output = %+v", out)
	}
	idx, err := store.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1 after importing edited projection from %s", len(idx.Conflicts), first.Claim.ID)
	}
}

func callTool(t *testing.T, srv *Server, name string, args map[string]any) map[string]any {
	t.Helper()
	out := callToolAny(t, srv, name, args)
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("tool output type = %T, want object: %+v", out, out)
	}
	return m
}

func callToolAny(t *testing.T, srv *Server, name string, args map[string]any) any {
	t.Helper()
	params := map[string]any{"name": name, "arguments": args}
	req := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}
	data, _ := json.Marshal(req)
	resp, ok := srv.Handle(data)
	if !ok {
		t.Fatal("no response")
	}
	var decoded struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Result.Content) != 1 {
		t.Fatalf("content len = %d", len(decoded.Result.Content))
	}
	var out any
	if err := json.Unmarshal([]byte(decoded.Result.Content[0].Text), &out); err != nil {
		t.Fatal(err)
	}
	if decoded.Result.IsError {
		t.Fatalf("tool error: %+v", out)
	}
	return out
}

func testStore(t *testing.T) *canon.Store {
	t.Helper()
	store, err := canon.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func mustString(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readFile(t *testing.T, store *canon.Store, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(storeRoot(t, store), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeFile(t *testing.T, store *canon.Store, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(storeRoot(t, store), name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func storeRoot(t *testing.T, store *canon.Store) string {
	t.Helper()
	return store.Root()
}
