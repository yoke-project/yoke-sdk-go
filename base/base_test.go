package base_test

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
)

// std: yoke-sdk-go:the-base.01
func TestTheBaseHoldsNothingOfOneContract(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	forbidden := []string{"Register", "Session", "Surface", "Heartbeat", "Stage", "Control", "Query", "Ack", "Data", "Health", "Event"}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range parsed.Imports {
			if strings.HasPrefix(strings.Trim(imp.Path.Value, `"`), "github.com/yoke-project/yoke-sdk-go/") {
				t.Errorf("%s imports the library %s", file, imp.Path.Value)
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "pluginv1" {
				for _, word := range forbidden {
					if strings.HasPrefix(sel.Sel.Name, word) || strings.HasPrefix(sel.Sel.Name, "Envelope_"+word) || strings.HasPrefix(sel.Sel.Name, "New"+word) {
						t.Errorf("%s refers to pluginv1.%s, which belongs to one contract", file, sel.Sel.Name)
					}
				}
			}
			return true
		})
	}
}

// std: yoke-sdk-go:the-base.02
func TestARefusalCarriesItsCodeAndEnvelopesAreNumbered(t *testing.T) {
	err := base.RefusalOf(&pluginv1.Error{Code: "session.correlation.unknown", Message: "names nothing"})
	var refusal *base.Refusal
	if !errors.As(err, &refusal) || refusal.Code != "session.correlation.unknown" || refusal.Message != "names nothing" {
		t.Fatalf("the refusal is %#v", err)
	}

	envelopes := base.NewEnvelopes("sid-1")
	seen := map[string]bool{}
	var first *pluginv1.Envelope
	for i := range 5 {
		e := envelopes.Seal(&pluginv1.Envelope{})
		if e.SessionId != "sid-1" || e.MessageId == "" || seen[e.MessageId] || e.SentAtUnixNano == 0 {
			t.Fatalf("envelope %d is %v", i, e)
		}
		seen[e.MessageId] = true
		if first == nil {
			first = e
		}
	}
	answer := envelopes.Answer(first.MessageId, &pluginv1.Envelope{})
	if answer.CorrelationId != first.MessageId || answer.CorrelationId == answer.MessageId {
		t.Errorf("the answer is %v, answering %s", answer, first.MessageId)
	}
}

// std: yoke-sdk-go:the-base.03
func TestTheAddressesComeFromTheEnvironment(t *testing.T) {
	values := map[string]string{
		"YOKE_PLUGIN": "com.yoke.station.acquire", "YOKE_UNIT": "acquire-1", "YOKE_SOCKET": "/run/yoke/plugin.sock",
		"YOKE_BIND": "/run/yoke/plugins/acquire-1.sock", "YOKE_TOKEN": "token",
	}
	env, err := base.Environment(func(k string) string { return values[k] })
	if err != nil {
		t.Fatal(err)
	}
	want := base.Env{Plugin: "com.yoke.station.acquire", Unit: "acquire-1", Socket: "/run/yoke/plugin.sock", Bind: "/run/yoke/plugins/acquire-1.sock", Token: "token"}
	if env != want {
		t.Errorf("read %+v, want %+v", env, want)
	}
	delete(values, "YOKE_SOCKET")
	if _, err := base.Environment(func(k string) string { return values[k] }); err == nil || !strings.Contains(err.Error(), "YOKE_SOCKET") {
		t.Errorf("a missing YOKE_SOCKET gave %v", err)
	}
}
