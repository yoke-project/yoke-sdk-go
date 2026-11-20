package plugin_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/plugin"
)

// std: yoke-sdk-go:the-plugin-library.21
func TestADeclarationSaysWhatItUsesAndWhatItRewrites(t *testing.T) {
	d := plugin.Declaration{
		ID:       "com.yoke.calendar",
		Needs:    []string{"unit:accounts", "secret:refresh"},
		Uses:     map[string]plugin.Use{"accounts": {Questions: []string{"oauth.token"}, Streams: []string{"oauth.status"}}},
		Rewrites: []string{"refresh"},
		Capabilities: []plugin.Capability{
			{Name: "query.oauth.token.ask", Governs: plugin.Object{Use: &plugin.UseObject{Need: "accounts", Query: "oauth.token"}}},
			{Name: "stream.oauth.status.follow", Governs: plugin.Object{Use: &plugin.UseObject{Need: "accounts", Stream: "oauth.status"}}},
		},
	}
	var m struct {
		Needs        []string `yaml:"needs"`
		Uses         map[string]map[string][]string
		Rewrites     []string
		Capabilities []struct {
			Name    string
			Governs map[string]map[string]string
		}
	}
	if err := yaml.Unmarshal(d.Manifest(), &m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Uses, map[string]map[string][]string{"accounts": {"questions": {"oauth.token"}, "streams": {"oauth.status"}}}) {
		t.Errorf("uses: %v", m.Uses)
	}
	if !reflect.DeepEqual(m.Rewrites, []string{"refresh"}) || !reflect.DeepEqual(m.Needs, []string{"unit:accounts", "secret:refresh"}) {
		t.Errorf("rewrites %v, needs %v", m.Rewrites, m.Needs)
	}
	if len(m.Capabilities) != 2 ||
		!reflect.DeepEqual(m.Capabilities[0].Governs, map[string]map[string]string{"use": {"need": "accounts", "query": "oauth.token"}}) ||
		!reflect.DeepEqual(m.Capabilities[1].Governs, map[string]map[string]string{"use": {"need": "accounts", "stream": "oauth.status"}}) {
		t.Errorf("capabilities: %v", m.Capabilities)
	}
	// A Plugin that uses nothing and rewrites nothing says neither.
	if plain := string(acquire().Manifest()); strings.Contains(plain, "uses:") || strings.Contains(plain, "rewrites:") || strings.Contains(plain, "use:") {
		t.Errorf("a declaration with no use generated\n%s", plain)
	}
}

// std: yoke-sdk-go:the-plugin-library.22
func TestAQuestionSurfacesWhoAskedItsDeadlineAndItsChain(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	deadline := time.Now().Add(30 * time.Second).Truncate(time.Millisecond)
	c.send(&pluginv1.Envelope{MessageId: "q-1", SessionId: "sid-1", Payload: &pluginv1.Envelope_Query{Query: &pluginv1.Query{
		Kind: &pluginv1.Query_Question_{Question: &pluginv1.Query_Question{Type: "head-status", Asker: "unit:archive",
			Deadline: uint64(deadline.UnixMilli()), Chain: "0123456789abcdef0123456789abcdef"}}}}})
	q, ok := (<-u.Events()).(plugin.Question)
	if !ok || q.ID != "q-1" || q.Asker != "unit:archive" || !q.Deadline.Equal(deadline) || q.Chain != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("surfaced %#v", q)
	}
	u.Answer(q, []byte("ok"))
	until(t, "the answer, carrying none of the three", func() bool {
		for _, e := range c.got() {
			if a := e.GetQuery().GetAnswer(); a != nil && e.CorrelationId == "q-1" {
				return true
			}
		}
		return false
	})
}

// std: yoke-sdk-go:the-plugin-library.23
func TestASecretTheUnitRewritesIsReplacedInPlace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "secrets", "refresh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Rewrite(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "new" {
		t.Fatalf("the value is %q, %v", got, err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Errorf("the value's mode is %v", info.Mode().Perm())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("the secret's directory holds %v", entries)
	}
	// A directory the unit may not write leaves the old value whole.
	os.Chmod(dir, 0o500)
	defer os.Chmod(dir, 0o700)
	if os.Geteuid() != 0 {
		if err := plugin.Rewrite(path, []byte("newer")); err == nil {
			t.Error("a rewrite in a directory the unit may not write succeeded")
		}
		if got, _ := os.ReadFile(path); string(got) != "new" {
			t.Errorf("a failed rewrite left %q", got)
		}
	}
}
