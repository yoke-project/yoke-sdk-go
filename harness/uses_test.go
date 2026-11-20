package harness_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/harness"
)

// std: yoke-sdk-go:the-harness.12
func TestAQuestionIsObservedWithWhoAskedItsDeadlineAndItsChain(t *testing.T) {
	c := &channel{}
	s, _, _ := started(t, c)
	s.directive(t, "start", nil)
	<-c.opened
	c.mu.Lock()
	c.send(&pluginv1.Envelope{MessageId: "c-1", SessionId: "sid-1", Payload: &pluginv1.Envelope_Query{Query: &pluginv1.Query{
		Kind: &pluginv1.Query_Question_{Question: &pluginv1.Query_Question{Type: "status", Asker: "operator:ada",
			Deadline: 1794760121000, Chain: "0123456789abcdef0123456789abcdef"}}}}})
	c.mu.Unlock()
	o := s.read(t)
	fields, _ := o["fields"].(map[string]any)
	if o["kind"] != "question" || fields["asker"] != "operator:ada" || fields["deadline"] != float64(1794760121000) ||
		fields["chain"] != "0123456789abcdef0123456789abcdef" {
		t.Errorf("the question was observed as %v", o)
	}
}

// std: yoke-sdk-go:the-harness.13
func TestTheSecretItRewritesIsDeclaredWhereTheSuiteNamesItAndRewritten(t *testing.T) {
	plain := harness.DeclarationFor(func(string) string { return "max_frame" })
	if len(plain.Rewrites) != 0 || strings.Contains(string(plain.Manifest()), "rewrites") {
		t.Fatalf("a suite naming no `rewrites` was described\n%s", plain.Manifest())
	}
	named := harness.DeclarationFor(func(k string) string { return map[string]string{"CONFORMANCE_FEATURES": "max_frame,rewrites"}[k] })
	if !slices.Equal(named.Rewrites, []string{"token"}) || !slices.Contains(named.Needs, "secret:token") {
		t.Fatalf("a suite naming `rewrites` was described %v, %v", named.Needs, named.Rewrites)
	}
	dir := filepath.Join(t.TempDir(), "token")
	os.MkdirAll(dir, 0o700)
	path := filepath.Join(dir, "value")
	os.WriteFile(path, []byte("first"), 0o600)
	c := &channel{}
	s, _, _ := started(t, c, "CONFORMANCE_FEATURES", "max_frame,rewrites", "CONFORMANCE_SECRET", path)
	s.directive(t, "start", nil)
	<-c.opened
	_, res := s.directive(t, "rewrite", map[string]any{"secret": "token", "value": "second"})
	if value, _ := res["value"].(map[string]any); res["refusal"] != nil || value["failed"] == true {
		t.Fatalf("rewrite answered %v", res)
	}
	if got, _ := os.ReadFile(path); string(got) != "second" {
		t.Fatalf("the secret holds %q", got)
	}
}
