package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jeffplourde/canon"
)

const protocolVersion = "2024-11-05"

type Server struct {
	store *canon.Store
}

func New(store *canon.Store) *Server {
	return &Server{store: store}
}

func (s *Server) Serve(r io.Reader, w io.Writer) error {
	br := bufio.NewReader(r)
	for {
		msg, err := readMessage(br)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		resp, ok := s.Handle(msg)
		if !ok {
			continue
		}
		if err := writeMessage(w, resp); err != nil {
			return err
		}
	}
}

func (s *Server) Handle(data []byte) ([]byte, bool) {
	var req request
	if err := json.Unmarshal(data, &req); err != nil {
		return mustJSON(response{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: "parse error"}}), true
	}
	if req.ID == nil {
		return nil, false
	}
	switch req.Method {
	case "initialize":
		return result(req.ID, map[string]any{
			"protocolVersion": protocolVersion,
			"serverInfo":      map[string]any{"name": "canon", "version": "0.1.0"},
			"capabilities":    map[string]any{"tools": map[string]any{}},
		}), true
	case "tools/list":
		return result(req.ID, map[string]any{"tools": toolDefinitions()}), true
	case "tools/call":
		out, isErr := s.callTool(req.Params)
		return result(req.ID, toolResult(out, isErr)), true
	default:
		return mustJSON(response{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: -32601, Message: "method not found"}}), true
	}
}

func (s *Server) callTool(raw json.RawMessage) (any, bool) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &call); err != nil {
		return map[string]string{"error": err.Error()}, true
	}
	switch call.Name {
	case "put_claim":
		var in canon.ClaimInput
		if err := json.Unmarshal(call.Arguments, &in); err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		out, err := s.store.PutClaim(in)
		if err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		return out, false
	case "resolve_conflict":
		var in struct {
			ConflictID       string `json:"conflict_id"`
			ChosenClaimID    string `json:"chosen_claim_id,omitempty"`
			SupersedingValue string `json:"superseding_value,omitempty"`
			Provenance       string `json:"provenance"`
		}
		if err := json.Unmarshal(call.Arguments, &in); err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		out, err := s.store.ResolveConflict(in.ConflictID, in.ChosenClaimID, in.SupersedingValue, in.Provenance)
		if err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		return out, false
	case "retract_claim":
		var in struct {
			ClaimID    string `json:"claim_id"`
			Provenance string `json:"provenance"`
		}
		if err := json.Unmarshal(call.Arguments, &in); err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		out, err := s.store.RetractClaim(in.ClaimID, in.Provenance)
		if err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		return out, false
	case "search":
		var in struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(call.Arguments, &in); err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		out, err := s.store.Search(in.Query)
		if err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		return out, false
	case "scan_external_changes":
		var in struct {
			Provenance string `json:"provenance,omitempty"`
		}
		if len(call.Arguments) > 0 {
			if err := json.Unmarshal(call.Arguments, &in); err != nil {
				return map[string]string{"error": err.Error()}, true
			}
		}
		out, err := s.store.ScanExternalChanges(in.Provenance)
		if err != nil {
			return map[string]string{"error": err.Error()}, true
		}
		return out, false
	default:
		return map[string]string{"error": "unknown tool " + call.Name}, true
	}
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func result(id json.RawMessage, value any) []byte {
	return mustJSON(response{JSONRPC: "2.0", ID: id, Result: value})
}

func toolResult(value any, isErr bool) map[string]any {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		data = []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
		isErr = true
	}
	return map[string]any{
		"content": []map[string]string{{"type": "text", "text": string(data)}},
		"isError": isErr,
	}
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func readMessage(r *bufio.Reader) ([]byte, error) {
	length := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("malformed header %q", line)
		}
		if strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return nil, fmt.Errorf("invalid Content-Length: %w", err)
			}
			length = n
		}
	}
	if length < 0 {
		return nil, fmt.Errorf("missing Content-Length")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeMessage(w io.Writer, data []byte) error {
	_, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n%s", len(data), data)
	return err
}

func EncodeForTest(data []byte) []byte {
	var b bytes.Buffer
	_ = writeMessage(&b, data)
	return b.Bytes()
}

func toolDefinitions() []map[string]any {
	return []map[string]any{
		{
			"name":        "put_claim",
			"description": "Record one claim. Equal values collapse; different values for the same canonical entity/key create a conflict object.",
			"inputSchema": objectSchema(map[string]any{
				"entity":     stringSchema("Entity the claim is about."),
				"key":        stringSchema("Fact key, normalized deterministically."),
				"value":      stringSchema("Scalar or short Markdown claim value."),
				"provenance": stringSchema("Who or what asserted this claim."),
				"base_hash":  stringSchema("Optional index hash for optimistic concurrency."),
			}, []string{"entity", "key", "value", "provenance"}),
		},
		{
			"name":        "resolve_conflict",
			"description": "Explicitly resolve a conflict by choosing one claim or recording a superseding value.",
			"inputSchema": objectSchema(map[string]any{
				"conflict_id":       stringSchema("Conflict id to resolve."),
				"chosen_claim_id":   stringSchema("Existing or incoming claim id to choose."),
				"superseding_value": stringSchema("Optional value that supersedes both sides."),
				"provenance":        stringSchema("Who or what resolved the conflict."),
			}, []string{"conflict_id", "provenance"}),
		},
		{
			"name":        "retract_claim",
			"description": "Retract a claim without deleting its event history.",
			"inputSchema": objectSchema(map[string]any{
				"claim_id":   stringSchema("Claim id to retract."),
				"provenance": stringSchema("Who or what retracted the claim."),
			}, []string{"claim_id", "provenance"}),
		},
		{
			"name":        "search",
			"description": "Search canonical, non-duplicate claims.",
			"inputSchema": objectSchema(map[string]any{
				"query": stringSchema("Substring query over entity, key, or value."),
			}, []string{"query"}),
		},
		{
			"name":        "scan_external_changes",
			"description": "Batch import safe hand edits from known canon claim blocks; ambiguous prose is recorded in the import inbox.",
			"inputSchema": objectSchema(map[string]any{
				"provenance": stringSchema("Optional provenance prefix for imported claims."),
			}, nil),
		},
	}
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	s := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
