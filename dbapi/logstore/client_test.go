package logstore_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	dblog "github.com/yoke-project/yoke-sdk-go/dbapi/logstore"

	_ "modernc.org/sqlite"
)

// seedLogDB creates a logs.db with the columns dbapi/logstore reads and
// returns its path. The schema mirrors the log store's log_entries and
// retention_policies tables (inlined here so the read client is tested
// against a known shape).
func seedLogDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "logs.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE log_entries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp_unix_ms INTEGER NOT NULL,
			plugin_id TEXT, source TEXT NOT NULL, level TEXT NOT NULL,
			level_order INTEGER NOT NULL DEFAULT 0,
			message TEXT NOT NULL, event_type TEXT, raw_json TEXT
		);
		CREATE TABLE retention_policies (
			plugin_id TEXT PRIMARY KEY, max_age_hours INTEGER,
			max_bytes INTEGER, max_entries INTEGER
		);
		INSERT INTO log_entries (timestamp_unix_ms, plugin_id, source, level, level_order, message)
		VALUES (1000, 'cpu', 'process', 'info', 1, 'hello'),
		       (2000, 'cpu', 'process', 'error', 3, 'boom'),
		       (3000, NULL, 'core', 'info', 1, 'core up');
		INSERT INTO retention_policies (plugin_id, max_age_hours, max_bytes, max_entries)
		VALUES ('cpu', 72, NULL, 1000);
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return path
}

func TestMissingDBErrors(t *testing.T) {
	// With mode=ro the lazy SQLite handle may let Open/Ping succeed on a
	// missing file; the error then surfaces on the first real query. Accept
	// either, but require that one of them fails.
	path := filepath.Join(t.TempDir(), "nope.db")
	c, err := dblog.Open(path)
	if err != nil {
		return // Open detected it — fine.
	}
	defer c.Close()
	if _, err := c.Query(context.Background(), dblog.QueryFilter{}); err == nil {
		t.Error("expected error from Query on a non-existent DB, got nil")
	}
}

func TestQueryAndCount(t *testing.T) {
	c, err := dblog.Open(seedLogDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	all, err := c.Query(ctx, dblog.QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("Query all = %d, want 3", len(all))
	}
	// Newest first.
	if all[0].Message != "core up" {
		t.Errorf("first message = %q, want 'core up'", all[0].Message)
	}

	// Filter by plugin.
	cpu, err := c.Query(ctx, dblog.QueryFilter{PluginID: "cpu"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(cpu) != 2 {
		t.Errorf("Query cpu = %d, want 2", len(cpu))
	}

	// Filter by min level (warn and above → only the error row).
	warn, err := c.Query(ctx, dblog.QueryFilter{MinLevel: dblog.LevelWarn})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(warn) != 1 || warn[0].Level != dblog.LevelError {
		t.Errorf("Query warn+ = %v, want one error row", warn)
	}

	n, err := c.Count(ctx, dblog.QueryFilter{Source: "process"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 2 {
		t.Errorf("Count process = %d, want 2", n)
	}
}

func TestListRetentionPolicies(t *testing.T) {
	c, err := dblog.Open(seedLogDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	pols, err := c.ListRetentionPolicies(context.Background())
	if err != nil {
		t.Fatalf("ListRetentionPolicies: %v", err)
	}
	if len(pols) != 1 || pols[0].PluginID != "cpu" {
		t.Fatalf("policies = %v, want one for cpu", pols)
	}
	if pols[0].MaxAgeHours == nil || *pols[0].MaxAgeHours != 72 {
		t.Errorf("MaxAgeHours = %v, want 72", pols[0].MaxAgeHours)
	}
	if pols[0].MaxBytes != nil {
		t.Errorf("MaxBytes = %v, want nil (NULL)", pols[0].MaxBytes)
	}
}
