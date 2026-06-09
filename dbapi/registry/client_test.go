package registry_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	dbreg "github.com/yoke-project/yoke-sdk-go/dbapi/registry"

	_ "modernc.org/sqlite"
)

// seedRegistryDB creates a registry.db with the tables and columns
// dbapi/registry reads (the three-layer join plus auth and lifecycle),
// inlined so the read client is tested against a known shape.
func seedRegistryDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE plugins (
			plugin_id TEXT PRIMARY KEY, display_name TEXT, version TEXT, description TEXT,
			endpoint TEXT, autostart INTEGER, manifest_hash TEXT,
			protocol_version TEXT, language TEXT
		);
		CREATE TABLE plugin_policy (
			plugin_id TEXT PRIMARY KEY, enabled INTEGER,
			authorized_capabilities TEXT, authorized_streams TEXT,
			authorized_commands TEXT, authorized_queries TEXT
		);
		CREATE TABLE plugin_runtime (
			plugin_id TEXT PRIMARY KEY, runtime_state TEXT, instance_id TEXT, session_id TEXT,
			restart_count INTEGER, active_stream_count INTEGER, degraded INTEGER, last_error_code TEXT,
			last_heartbeat_at INTEGER, last_registration_at INTEGER,
			last_start_at INTEGER, last_stop_at INTEGER, last_crash_at INTEGER
		);
		CREATE TABLE plugin_authentication (
			plugin_id TEXT PRIMARY KEY, auth_mode TEXT, credential_fingerprint TEXT,
			last_successful_auth_at INTEGER, last_rejected_auth_at INTEGER,
			last_rejection_code TEXT, updated_at INTEGER
		);
		CREATE TABLE plugin_lifecycle_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT, plugin_id TEXT, instance_id TEXT,
			event_type TEXT, event_code TEXT, summary TEXT, created_at INTEGER
		);

		INSERT INTO plugins VALUES
			('cpu', 'CPU Monitor', '0.1.0', 'desc', '/run/cpu.sock', 1, 'h1', '1.0', 'go');
		INSERT INTO plugin_policy VALUES
			('cpu', 1, '["stream:system"]', '["cpu.usage"]', '["StartStream"]', '["q"]');
		INSERT INTO plugin_runtime
			(plugin_id, runtime_state, instance_id, session_id, restart_count, active_stream_count, degraded, last_error_code)
		VALUES ('cpu', 'Idle', 'inst-1', 'sess-1', 2, 1, 0, '');
		INSERT INTO plugin_authentication (plugin_id, auth_mode, updated_at)
		VALUES ('cpu', 'bootstrap', 5000);
		INSERT INTO plugin_lifecycle_events (plugin_id, instance_id, event_type, event_code, summary, created_at)
		VALUES ('cpu', 'inst-1', 'registration_accepted', '', '', 100),
		       ('cpu', 'inst-1', 'heartbeat_missed', 'ERR', 'missed', 200);
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return path
}

func TestListAndGetPlugin(t *testing.T) {
	c, err := dbreg.Open(seedRegistryDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	list, err := c.ListPlugins(ctx)
	if err != nil {
		t.Fatalf("ListPlugins: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListPlugins = %d, want 1", len(list))
	}

	p, err := c.GetPlugin(ctx, "cpu")
	if err != nil {
		t.Fatalf("GetPlugin: %v", err)
	}
	if p.DisplayName != "CPU Monitor" || !p.Enabled || !p.Autostart {
		t.Errorf("unexpected record: %+v", p)
	}
	if p.RuntimeState != "Idle" || p.RestartCount != 2 || p.ActiveStreamCount != 1 {
		t.Errorf("unexpected runtime fields: %+v", p)
	}
	if len(p.AuthorizedCapabilities) != 1 || p.AuthorizedCapabilities[0] != "stream:system" {
		t.Errorf("AuthorizedCapabilities = %v", p.AuthorizedCapabilities)
	}
	if len(p.AuthorizedQueries) != 1 || p.AuthorizedQueries[0] != "q" {
		t.Errorf("AuthorizedQueries = %v", p.AuthorizedQueries)
	}
}

func TestGetPluginNotFound(t *testing.T) {
	c, err := dbreg.Open(seedRegistryDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	_, err = c.GetPlugin(context.Background(), "missing")
	if !errors.Is(err, dbreg.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestGetAuthRecordAndLifecycle(t *testing.T) {
	c, err := dbreg.Open(seedRegistryDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	auth, err := c.GetAuthRecord(ctx, "cpu")
	if err != nil {
		t.Fatalf("GetAuthRecord: %v", err)
	}
	if auth.AuthMode != "bootstrap" || auth.LastSuccessfulAuthAt != nil {
		t.Errorf("unexpected auth record: %+v", auth)
	}

	events, err := c.ListLifecycleEvents(ctx, "cpu", 0)
	if err != nil {
		t.Fatalf("ListLifecycleEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	// Newest first.
	if events[0].EventType != "heartbeat_missed" {
		t.Errorf("events[0].EventType = %q, want heartbeat_missed", events[0].EventType)
	}

	if _, err := c.GetAuthRecord(ctx, "missing"); !errors.Is(err, dbreg.ErrNotFound) {
		t.Errorf("GetAuthRecord(missing) err = %v, want ErrNotFound", err)
	}
}
