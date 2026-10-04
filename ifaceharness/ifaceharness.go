// Package ifaceharness is the Go interface library's harness: the thinnest translation between the
// suite's directives and the library. Launched by the suite against an instance, it attaches to the
// channel a directive names at the address the library computes, turns each further directive into one
// call of the library, reports what the library answered in the suite's vocabulary — a refusal as its
// code and what it names, never the library's words — and what the standing subscription or a
// subscription delivers as observations, in order. It judges nothing, confirms only when told to, and
// speaks no wire of its own.
package ifaceharness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/iface"
)

type line struct {
	Type         string         `json:"type"`
	Contract     string         `json:"contract,omitempty"`
	Language     string         `json:"language,omitempty"`
	SDK          string         `json:"sdk,omitempty"`
	Version      int            `json:"version,omitempty"`
	ID           any            `json:"id,omitempty"`
	Verb         string         `json:"verb,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
	Value        map[string]any `json:"value,omitempty"`
	Refusal      string         `json:"refusal,omitempty"`
	Unrecognised bool           `json:"unrecognised,omitempty"`
	Kind         string         `json:"kind,omitempty"`
	Fields       map[string]any `json:"fields,omitempty"`
}

// plain is a message of the contract as the suite reads it: its fields by their names in the contract.
func plain(m proto.Message) map[string]any {
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil {
		return map[string]any{"unreadable": err.Error()}
	}
	var out map[string]any
	json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func records(rs []*interfacev1.Record) []any {
	out := []any{}
	for _, r := range rs {
		out = append(out, plain(r))
	}
	return out
}

func text(args map[string]any, key string) string {
	s, _ := args[key].(string)
	return s
}

// number reads a number the suite sent as a number, or as the string a 64-bit value is written as.
func number(args map[string]any, key string) uint64 {
	switch n := args[key].(type) {
	case float64:
		return uint64(n)
	case string:
		var v uint64
		json.Unmarshal([]byte(n), &v)
		return v
	}
	return 0
}

// event is an event as the suite reads it.
func event(e *interfacev1.Event) map[string]any {
	return map[string]any{"seq": e.GetSeq(), "type": e.GetType(), "subject_kind": e.GetSubject().GetKind(),
		"subject": e.GetSubject().GetIdentity(), "severity": e.GetSeverity()}
}

// harness is what the harness holds between directives: its attachment, once one is made.
type harness struct {
	getenv  func(string) string
	held    context.Context
	observe func(string, map[string]any)
	a       *iface.Attachment
	extra   []*iface.Attachment
}

// Serve runs the harness until it is told to finish, and returns its exit status.
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
	send(line{Type: "hello", Contract: "interface", Language: "go", SDK: base.SDKLine, Version: iface.Contract})

	// Every attachment and subscription lives until the harness finishes.
	held, release := context.WithCancel(ctx)
	defer release()
	h := &harness{getenv: getenv, held: held,
		observe: func(kind string, fields map[string]any) { send(line{Type: "observation", Kind: kind, Fields: fields}) }}
	defer h.close()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var d line
		if json.Unmarshal(scanner.Bytes(), &d) != nil {
			continue
		}
		if d.Type == "finish" {
			return 0
		}
		if d.Type != "directive" {
			continue
		}
		result := h.act(d.Verb, d.Args)
		result.Type, result.ID = "result", d.ID
		send(result)
	}
	return 0
}

func (h *harness) close() {
	for _, a := range append(h.extra, h.a) {
		if a != nil {
			a.Close()
		}
	}
}

// subscribed reports what a subscription delivers, in order, until it ends.
func (h *harness) subscribed(sub *iface.Subscription) {
	for {
		d, err := sub.Next()
		if err != nil {
			h.observe("ended", map[string]any{})
			return
		}
		switch {
		case d.GetSnapshot() != nil:
			h.observe("snapshot", map[string]any{"at": d.GetSnapshot().GetAt(), "records": records(d.GetSnapshot().GetRecords())})
		case d.GetOverflow() != nil:
			h.observe("overflow", map[string]any{"at": d.GetOverflow().GetAt(), "records": records(d.GetOverflow().GetRecords())})
		case d.GetEvent() != nil:
			h.observe("event", event(d.GetEvent()))
		}
	}
}

// attach attaches to the channel named, at the address the library computes under the instance.
func (h *harness) attach(ctx context.Context, channel string) line {
	address, err := iface.Service(h.getenv("CONFORMANCE_INSTANCE"), channel)
	if err != nil {
		return line{Value: map[string]any{"failed": true}}
	}
	a, err := iface.Attach(ctx, address)
	if err != nil {
		return refused(err)
	}
	// A second attachment the channel admitted is kept beside the first, which stays the one directed.
	if h.a != nil {
		h.extra = append(h.extra, a)
	} else {
		h.a = a
		if a.Standing != nil {
			go h.subscribed(a.Standing)
		}
	}
	standing := ""
	if a.Standing != nil {
		standing = a.Standing.ID
	}
	return line{Value: map[string]any{"version": a.Version, "subscription": standing, "at": a.Picture.GetAt(),
		"records": records(a.Picture.GetRecords())}}
}

// act performs one directive, and reports what the library answered.
func (h *harness) act(verb string, args map[string]any) line {
	call, cancel := context.WithTimeout(h.held, 30*time.Second)
	defer cancel()
	if verb == "attach" {
		return h.attach(call, text(args, "channel"))
	}
	a := h.a
	switch verb {
	case "authenticate", "read", "subscribe", "confirm", "command", "query", "stream-start", "stream-stop", "stream-subscribe", "reclaim":
		if a == nil {
			return line{Value: map[string]any{"failed": true, "attached": false}}
		}
	}
	switch verb {
	case "authenticate":
		var got *interfacev1.Authenticated
		var err error
		if token := text(args, "token"); token != "" {
			got, err = a.AuthenticateToken(call, token)
		} else {
			got, err = a.Authenticate(call, text(args, "account"), text(args, "secret"))
		}
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(got)}
	case "read":
		rs, err := a.Read(call, text(args, "kind"), text(args, "identity"))
		if err != nil {
			return refused(err)
		}
		return line{Value: map[string]any{"records": records(rs)}}
	case "subscribe":
		sub, err := a.Subscribe(call, &interfacev1.Filter{SubjectKind: text(args, "subject_kind"), SubjectIdentity: text(args, "subject_identity"),
			Type: text(args, "type")})
		if err != nil {
			return refused(err)
		}
		go h.subscribed(sub)
		return line{Value: map[string]any{}}
	case "confirm":
		subscription := text(args, "subscription")
		if subscription == "" && a.Standing != nil {
			subscription = a.Standing.ID
		}
		if err := a.Confirm(call, subscription, number(args, "sequence")); err != nil {
			return refused(err)
		}
		return line{Value: map[string]any{}}
	case "command":
		ack, err := a.Command(call, text(args, "unit"), text(args, "type"), []byte(text(args, "payload")))
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(ack)}
	case "query":
		answer, err := a.Ask(call, text(args, "unit"), text(args, "type"), []byte(text(args, "payload")))
		if err != nil {
			return refused(err)
		}
		return line{Value: map[string]any{"answer": string(answer)}}
	case "stream-start":
		ack, err := a.StartStream(call, text(args, "unit"), text(args, "stream"))
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(ack)}
	case "stream-stop":
		ack, err := a.StopStream(call, text(args, "unit"), text(args, "stream"))
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(ack)}
	case "stream-subscribe":
		d, err := a.SubscribeStream(call, text(args, "unit"), text(args, "stream"))
		if err != nil {
			return refused(err)
		}
		go func() {
			for {
				f, err := d.Next()
				if err != nil {
					h.observe("delivery-ended", map[string]any{"delivery": d.ID})
					return
				}
				h.observe("frame", map[string]any{"delivery": d.ID, "sequence": f.Sequence, "sent_at": f.SentAt, "payload": string(f.Payload)})
			}
		}()
		return line{Value: map[string]any{"delivery": d.ID, "flowing": d.Flowing}}
	case "reclaim":
		got, err := a.Reclaim(call)
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(got)}
	}
	return line{Unrecognised: true}
}

// refused is a refusal as its code and what it names; anything else the library failed with is a failure
// the harness does not interpret.
func refused(err error) line {
	var r *base.Refusal
	if !errors.As(err, &r) {
		return line{Value: map[string]any{"failed": true}}
	}
	value := map[string]any{}
	if r.Subject.Kind != "" {
		value["subject"] = map[string]any{"kind": r.Subject.Kind, "identity": r.Subject.Identity, "incarnation": r.Subject.Incarnation}
	}
	if r.Item != "" {
		value["item"] = r.Item
	}
	if grade, by, ok := iface.SuspensionOf(err); ok {
		value["suspension"] = map[string]any{"grade": grade, "by": by}
	}
	return line{Refusal: r.Code, Value: value}
}
