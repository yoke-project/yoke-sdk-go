// Package adminharness is the Go administrative library's harness: the thinnest translation between the
// suite's directives and the library. Launched by the suite against an instance, it turns a directive
// into one call of the library on the operator projection, reports what the library answered in the
// suite's vocabulary — a refusal as its code and what it names, never the library's words — and what a
// subscription or a follow delivers as observations, in order. It judges nothing and speaks no wire of
// its own.
package adminharness

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

	administrativev1 "github.com/yoke-project/yoke/proto/yoke/administrative/v1"

	"github.com/yoke-project/yoke-sdk-go/admin"
	"github.com/yoke-project/yoke-sdk-go/base"
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

func text(args map[string]any, key string) string {
	s, _ := args[key].(string)
	return s
}

func number(args map[string]any, key string) uint64 {
	n, _ := args[key].(float64)
	return uint64(n)
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
	send(line{Type: "hello", Contract: "administrative", Language: "go", SDK: base.SDKLine, Version: admin.Contract})

	operator, err := admin.DialOperator(admin.Service(getenv("CONFORMANCE_INSTANCE")).Operator)
	if err != nil {
		return 1
	}
	defer operator.Close()
	// Every subscription and follow lives until the harness finishes.
	held, release := context.WithCancel(ctx)
	defer release()
	observe := func(kind string, fields map[string]any) { send(line{Type: "observation", Kind: kind, Fields: fields}) }

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
		result := act(held, operator.Operations, d.Verb, d.Args, observe)
		result.Type, result.ID = "result", d.ID
		send(result)
	}
	return 0
}

// act performs one directive, and reports what the library answered.
func act(ctx context.Context, o admin.Operations, verb string, args map[string]any, observe func(string, map[string]any)) line {
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var changed admin.Changed
	var err error
	switch verb {
	case "enable":
		changed, err = o.EnablePlugin(call, text(args, "plugin"))
	case "disable":
		changed, err = o.DisablePlugin(call, text(args, "plugin"))
	case "grant":
		changed, err = o.Grant(call, text(args, "plugin"), text(args, "capability"))
	case "withdraw":
		changed, err = o.Withdraw(call, text(args, "plugin"), text(args, "capability"))
	case "start-unit":
		changed, err = o.StartUnit(call, text(args, "unit"))
	case "stop-unit":
		changed, err = o.StopUnit(call, text(args, "unit"))
	case "restart-unit":
		changed, err = o.RestartUnit(call, text(args, "unit"))
	case "start-stream":
		changed, err = o.StartStream(call, text(args, "unit"), text(args, "stream"))
	case "stop-stream":
		changed, err = o.StopStream(call, text(args, "unit"), text(args, "stream"))
	case "set-retention":
		r := admin.Retention{Age: time.Duration(number(args, "age_seconds")) * time.Second}
		if n, ok := args["bytes"].(float64); ok {
			b := uint64(n)
			r.Bytes = &b
		}
		if n, ok := args["entries"].(float64); ok {
			e := uint64(n)
			r.Entries = &e
		}
		changed, err = o.SetRetention(call, text(args, "unit"), r)
	case "clear-retention":
		changed, err = o.ClearRetention(call, text(args, "unit"))
	case "ask":
		answer, err := o.Ask(call, text(args, "unit"), text(args, "type"), []byte(text(args, "question")))
		if err != nil {
			return refused(err)
		}
		return line{Value: map[string]any{"answer": string(answer)}}
	case "read":
		records, err := o.Read(call, text(args, "kind"), text(args, "identity"))
		if err != nil {
			return refused(err)
		}
		out := []any{}
		for _, r := range records {
			out = append(out, plain(r))
		}
		return line{Value: map[string]any{"records": out}}
	case "query-log":
		page, err := o.QueryLog(call, &administrativev1.LogQuery{Unit: text(args, "unit"), Incarnation: number(args, "incarnation"),
			Floor: uint32(number(args, "floor")), Cursor: number(args, "cursor")})
		if err != nil {
			return refused(err)
		}
		return line{Value: plain(page)}
	case "subscribe":
		sub, err := o.Subscribe(ctx, &administrativev1.Filter{SubjectKind: text(args, "subject_kind"), SubjectIdentity: text(args, "subject_identity"),
			Floor: uint32(number(args, "floor")), Type: text(args, "type"), TypePrefix: args["type_prefix"] == true})
		if err != nil {
			return refused(err)
		}
		go func() {
			for {
				d, err := sub.Next()
				if err != nil {
					observe("ended", map[string]any{})
					return
				}
				switch {
				case d.GetSnapshot() != nil:
					observe("snapshot", plain(d.GetSnapshot()))
				case d.GetOverflow() != nil:
					observe("overflow", plain(d.GetOverflow()))
				case d.GetEvent() != nil:
					e := d.GetEvent()
					observe("event", map[string]any{"seq": e.GetSeq(), "type": e.GetType(), "subject_kind": e.GetSubject().GetKind(),
						"subject": e.GetSubject().GetIdentity(), "actor": e.GetActor().GetClass(), "person": e.GetActor().GetPerson(), "severity": e.GetSeverity()})
				}
			}
		}()
		return line{Value: map[string]any{}}
	case "follow-log":
		f, err := o.FollowLog(ctx, &administrativev1.LogFollow{Unit: text(args, "unit"), Incarnation: number(args, "incarnation"),
			Floor: uint32(number(args, "floor")), Cursor: number(args, "cursor")})
		if err != nil {
			return refused(err)
		}
		go func() {
			for {
				d, err := f.Next()
				if err != nil {
					observe("ended", map[string]any{})
					return
				}
				if e := d.GetEntry(); e != nil {
					observe("entry", plain(e))
				} else {
					observe("behind", map[string]any{"at": d.GetBehindAt()})
				}
			}
		}()
		return line{Value: map[string]any{}}
	default:
		return line{Unrecognised: true}
	}
	if err != nil {
		return refused(err)
	}
	return reported(changed)
}

// reported is a change as the suite reads it.
func reported(c admin.Changed) line {
	effective := map[admin.Effective]string{admin.Immediately: "immediately", admin.AtNextAdmission: "at next admission"}[c.Effective]
	consequences := []any{}
	for _, q := range c.Consequences {
		consequences = append(consequences, map[string]any{"unit": q.Unit, "incarnation": q.Incarnation, "what": q.What})
	}
	return line{Value: map[string]any{"previously": plain(c.Previously), "effective": effective, "consequences": consequences}}
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
	return line{Refusal: r.Code, Value: value}
}
