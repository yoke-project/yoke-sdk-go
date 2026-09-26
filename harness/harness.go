// Package harness is the Go plugin library's harness: the thinnest translation between the suite's
// directives and the library. It turns a directive into a library call and what the library surfaces
// into an observation, reports a refusal as its code and a verb it does not know as unrecognised, and
// judges nothing: what a case requires lives in the suite. It speaks no wire of its own.
package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/plugin"
)

// Declaration is what the harness declares: one object of every kind, each governed by a capability.
func Declaration() plugin.Declaration {
	return plugin.Declaration{
		ID:          "com.yoke.conformance.go",
		Streams:     []plugin.Stream{{ID: "conformance.data"}},
		Commands:    []string{"calibrate"},
		Queries:     []string{"status"},
		Occurrences: []string{"conformance.drift"},
		Capabilities: []plugin.Capability{
			{Name: "stream.data.publish", Governs: plugin.Object{Stream: "conformance.data"}},
			{Name: "command.calibrate.accept", Governs: plugin.Object{Command: "calibrate"}},
			{Name: "query.status.answer", Governs: plugin.Object{Query: "status"}},
			{Name: "event.drift.report", Governs: plugin.Object{Occurrence: "conformance.drift"}},
		},
	}
}

type line struct {
	Type         string         `json:"type"`
	Contract     string         `json:"contract,omitempty"`
	Language     string         `json:"language,omitempty"`
	SDK          string         `json:"sdk,omitempty"`
	Version      int            `json:"version,omitempty"`
	Unit         string         `json:"unit,omitempty"`
	ID           string         `json:"id,omitempty"`
	Verb         string         `json:"verb,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
	Value        map[string]any `json:"value,omitempty"`
	Refusal      string         `json:"refusal,omitempty"`
	Unrecognised bool           `json:"unrecognised,omitempty"`
	Kind         string         `json:"kind,omitempty"`
	Fields       map[string]any `json:"fields,omitempty"`
}

// Serve runs the harness until it is told to finish or its Session ends, and returns its exit status.
func Serve(ctx context.Context, getenv func(string) string) int {
	conn, err := net.Dial("unix", getenv("CONFORMANCE_SOCKET"))
	if err != nil {
		return 1
	}
	defer conn.Close()
	var mu sync.Mutex
	send := func(l line) {
		b, _ := json.Marshal(l)
		mu.Lock()
		conn.Write(append(b, '\n'))
		mu.Unlock()
	}
	send(line{Type: "hello", Contract: "plugin", Language: "go", SDK: plugin.SDKLine, Version: base.PluginContract, Unit: getenv("YOKE_UNIT")})

	h := &state{commands: map[string]plugin.Command{}, questions: map[string]plugin.Question{}, ended: make(chan struct{})}
	directives := make(chan line)
	go func() {
		defer close(directives)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var l line
			if json.Unmarshal(scanner.Bytes(), &l) == nil {
				directives <- l
			}
		}
	}()
	for {
		select {
		case <-h.ended:
			// The Session ended: the incarnation is over, and so is the process.
			return 0
		case d, open := <-directives:
			if !open || d.Type == "finish" {
				if h.unit != nil {
					h.unit.Close()
				}
				return 0
			}
			if d.Type != "directive" {
				continue
			}
			result := h.do(ctx, d, getenv, send)
			result.Type, result.ID = "result", d.ID
			send(result)
		}
	}
}

type state struct {
	unit      *plugin.Unit
	commands  map[string]plugin.Command
	questions map[string]plugin.Question
	mu        sync.Mutex
	ended     chan struct{}
}

// refusal reports an error as its code, and never as the library's message.
func refusal(err error) line {
	var r *base.Refusal
	if errors.As(err, &r) {
		l := line{Refusal: r.Code}
		if r.Stage != "" {
			l.Value = map[string]any{"stage": r.Stage}
		}
		return l
	}
	return line{Value: map[string]any{"failed": true}}
}

func (h *state) do(ctx context.Context, d line, getenv func(string) string, send func(line)) line {
	arg := func(name string) string { s, _ := d.Args[name].(string); return s }
	switch d.Verb {
	case "describe":
		return line{Value: map[string]any{"manifest": string(Declaration().Manifest())}}
	case "start":
		u, err := plugin.StartWith(ctx, Declaration(), getenv)
		if err != nil {
			return refusal(err)
		}
		if h.unit == nil {
			h.unit = u
			go h.observe(u, send)
		}
		a := u.Admission()
		outcome := "accepted"
		if a.Restricted {
			outcome = "accepted with restrictions"
		}
		scope := func(s plugin.Scope) map[string]any {
			return map[string]any{"capabilities": s.Capabilities, "streams": s.Streams, "commands": s.Commands, "queries": s.Queries}
		}
		return line{Value: map[string]any{"outcome": outcome, "granted": scope(a.Granted), "withheld": scope(a.Withheld)}}
	}
	if h.unit == nil {
		if d.Verb == "close" || d.Verb == "emit" || d.Verb == "report-health" || d.Verb == "report" || d.Verb == "ack" || d.Verb == "answer" {
			return line{Value: map[string]any{"failed": true, "started": false}}
		}
		return line{Unrecognised: true}
	}
	var err error
	switch d.Verb {
	case "close":
		err = h.unit.Close()
	case "emit":
		err = h.unit.Emit(arg("stream"), []byte(arg("payload")))
	case "report-health":
		grade, _ := d.Args["grade"].(float64)
		err = h.unit.Health(uint8(grade), arg("line"))
	case "report":
		severity := plugin.Severity{}
		if n, ok := d.Args["severity"].(float64); ok {
			severity = plugin.SeverityOf(uint8(n))
		}
		err = h.unit.Report(arg("occurrence"), severity, arg("line"), nil)
	case "ack":
		h.mu.Lock()
		c := h.commands[arg("command")]
		h.mu.Unlock()
		err = h.unit.Ack(c, plugin.Done, arg("line"))
	case "answer":
		h.mu.Lock()
		q := h.questions[arg("question")]
		h.mu.Unlock()
		err = h.unit.Answer(q, []byte(arg("payload")))
	default:
		return line{Unrecognised: true}
	}
	if err != nil {
		return refusal(err)
	}
	return line{Value: map[string]any{}}
}

// observe reports everything the library surfaces, in the order it surfaced it.
func (h *state) observe(u *plugin.Unit, send func(line)) {
	for e := range u.Events() {
		switch e := e.(type) {
		case plugin.Command:
			h.mu.Lock()
			h.commands[e.ID] = e
			h.mu.Unlock()
			send(line{Type: "observation", Kind: "command", Fields: map[string]any{"id": e.ID, "type": e.Type}})
		case plugin.Question:
			h.mu.Lock()
			h.questions[e.ID] = e
			h.mu.Unlock()
			send(line{Type: "observation", Kind: "question", Fields: map[string]any{"id": e.ID, "type": e.Type}})
		case plugin.Activated:
			send(line{Type: "observation", Kind: "activated", Fields: map[string]any{"stream": e.Stream, "transport": e.Transport}})
		case plugin.Stopped:
			send(line{Type: "observation", Kind: "stopped", Fields: map[string]any{"stream": e.Stream}})
		case plugin.Refused:
			code := ""
			var r *base.Refusal
			if errors.As(e.Err, &r) {
				code = r.Code
			}
			send(line{Type: "observation", Kind: "refused", Fields: map[string]any{"correlation": e.Correlation, "code": code}})
		case plugin.Ended:
			send(line{Type: "observation", Kind: "session-ended", Fields: map[string]any{"closed": e.Closed, "cause": e.Cause}})
			close(h.ended)
			return
		}
	}
}
