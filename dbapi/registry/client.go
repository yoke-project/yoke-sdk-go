// Package registry is the read-only Go client for the Yoke plugin
// registry database. Open returns a Client that exposes typed list and
// get methods; the SQLite file is opened in mode=ro so callers can run
// concurrently with a writing yoke-core.
package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by Get* methods when no row matches.
var ErrNotFound = errors.New("dbapi/registry: not found")

// Client is a read-only handle to a registry database file.
type Client struct {
	db *sql.DB
}

// Open opens path in read-only mode. Callers own the Client and must
// Close it when done.
func Open(path string) (*Client, error) {
	conn, err := sql.Open("sqlite", path+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("dbapi/registry: open %s: %w", path, err)
	}
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dbapi/registry: ping %s: %w", path, err)
	}
	return &Client{db: conn}, nil
}

// Close releases the underlying database handle.
func (c *Client) Close() error { return c.db.Close() }

// ListPlugins returns every known plugin ordered by plugin_id.
func (c *Client) ListPlugins(ctx context.Context) ([]*PluginRecord, error) {
	rows, err := c.db.QueryContext(ctx, pluginQuery+` ORDER BY p.plugin_id`)
	if err != nil {
		return nil, fmt.Errorf("dbapi/registry: list plugins: %w", err)
	}
	defer rows.Close()

	var out []*PluginRecord
	for rows.Next() {
		rec, err := scanPlugin(rows)
		if err != nil {
			return nil, fmt.Errorf("dbapi/registry: list plugins scan: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// GetPlugin returns one plugin record. Wraps ErrNotFound when missing.
func (c *Client) GetPlugin(ctx context.Context, pluginID string) (*PluginRecord, error) {
	row := c.db.QueryRowContext(ctx, pluginQuery+` WHERE p.plugin_id = ?`, pluginID)
	rec, err := scanPlugin(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("dbapi/registry: get %q: %w", pluginID, ErrNotFound)
		}
		return nil, fmt.Errorf("dbapi/registry: get %q: %w", pluginID, err)
	}
	return rec, nil
}

// ListLifecycleEvents returns up to limit lifecycle events for the
// given plugin, newest first. limit ≤ 0 returns all rows.
func (c *Client) ListLifecycleEvents(ctx context.Context, pluginID string, limit int) ([]*LifecycleEvent, error) {
	q := `
		SELECT id, plugin_id, COALESCE(instance_id, ''), event_type,
		       COALESCE(event_code, ''), COALESCE(summary, ''), created_at
		FROM plugin_lifecycle_events
		WHERE plugin_id = ?
		ORDER BY created_at DESC, id DESC`
	args := []any{pluginID}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dbapi/registry: list lifecycle events: %w", err)
	}
	defer rows.Close()

	var out []*LifecycleEvent
	for rows.Next() {
		var (
			ev        LifecycleEvent
			createdMs int64
		)
		if err := rows.Scan(&ev.ID, &ev.PluginID, &ev.InstanceID, &ev.EventType,
			&ev.EventCode, &ev.Summary, &createdMs); err != nil {
			return nil, fmt.Errorf("dbapi/registry: scan lifecycle event: %w", err)
		}
		ev.CreatedAt = time.UnixMilli(createdMs).UTC()
		out = append(out, &ev)
	}
	return out, rows.Err()
}

// GetAuthRecord returns the authentication metadata for pluginID.
func (c *Client) GetAuthRecord(ctx context.Context, pluginID string) (*AuthRecord, error) {
	row := c.db.QueryRowContext(ctx, `
		SELECT plugin_id, auth_mode, COALESCE(credential_fingerprint, ''),
		       last_successful_auth_at, last_rejected_auth_at,
		       COALESCE(last_rejection_code, ''), updated_at
		FROM plugin_authentication WHERE plugin_id = ?`, pluginID)

	var (
		rec        AuthRecord
		successMs  sql.NullInt64
		rejectedMs sql.NullInt64
		updatedMs  int64
	)
	err := row.Scan(
		&rec.PluginID, &rec.AuthMode, &rec.CredentialFingerprint,
		&successMs, &rejectedMs, &rec.LastRejectionCode, &updatedMs,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("dbapi/registry: get auth %q: %w", pluginID, ErrNotFound)
		}
		return nil, fmt.Errorf("dbapi/registry: get auth %q: %w", pluginID, err)
	}
	if successMs.Valid {
		t := time.UnixMilli(successMs.Int64).UTC()
		rec.LastSuccessfulAuthAt = &t
	}
	if rejectedMs.Valid {
		t := time.UnixMilli(rejectedMs.Int64).UTC()
		rec.LastRejectedAuthAt = &t
	}
	rec.UpdatedAt = time.UnixMilli(updatedMs).UTC()
	return &rec, nil
}

// pluginQuery joins the declarative, authorization, and runtime layers.
// Append a WHERE / ORDER BY clause before executing.
const pluginQuery = `
	SELECT
		p.plugin_id, p.display_name, p.version, p.description,
		p.endpoint, p.autostart, p.manifest_hash, p.protocol_version, p.language,
		pp.enabled, pp.authorized_capabilities, pp.authorized_streams, pp.authorized_commands, pp.authorized_queries,
		pr.runtime_state,
		COALESCE(pr.instance_id, ''),    COALESCE(pr.session_id, ''),
		pr.restart_count,
		COALESCE(pr.active_stream_count, 0),
		COALESCE(pr.degraded, 0),
		COALESCE(pr.last_error_code, ''),
		pr.last_heartbeat_at,
		pr.last_registration_at,
		pr.last_start_at,
		pr.last_stop_at,
		pr.last_crash_at
	FROM plugins p
	JOIN plugin_policy  pp ON pp.plugin_id = p.plugin_id
	JOIN plugin_runtime pr ON pr.plugin_id = p.plugin_id`

type scanner interface {
	Scan(dest ...any) error
}

func scanPlugin(s scanner) (*PluginRecord, error) {
	var (
		rec         PluginRecord
		autostart   int
		enabled     int
		degraded    int
		capsJSON    string
		streamsJSON string
		cmdsJSON    string
		queriesJSON string
		lastHbMs    sql.NullInt64
		lastRegMs   sql.NullInt64
		lastStartMs sql.NullInt64
		lastStopMs  sql.NullInt64
		lastCrashMs sql.NullInt64
	)
	err := s.Scan(
		&rec.PluginID, &rec.DisplayName, &rec.Version, &rec.Description,
		&rec.Endpoint, &autostart, &rec.ManifestHash, &rec.ProtocolVersion, &rec.Language,
		&enabled, &capsJSON, &streamsJSON, &cmdsJSON, &queriesJSON,
		&rec.RuntimeState, &rec.InstanceID, &rec.SessionID,
		&rec.RestartCount, &rec.ActiveStreamCount, &degraded, &rec.LastErrorCode,
		&lastHbMs, &lastRegMs, &lastStartMs, &lastStopMs, &lastCrashMs,
	)
	if err != nil {
		return nil, err
	}

	rec.Autostart = autostart == 1
	rec.Enabled = enabled == 1
	rec.Degraded = degraded == 1

	_ = json.Unmarshal([]byte(capsJSON), &rec.AuthorizedCapabilities)
	_ = json.Unmarshal([]byte(streamsJSON), &rec.AuthorizedStreams)
	_ = json.Unmarshal([]byte(cmdsJSON), &rec.AuthorizedCommands)
	_ = json.Unmarshal([]byte(queriesJSON), &rec.AuthorizedQueries)

	msToTime := func(n sql.NullInt64) *time.Time {
		if !n.Valid {
			return nil
		}
		t := time.UnixMilli(n.Int64).UTC()
		return &t
	}
	rec.LastHeartbeat = msToTime(lastHbMs)
	rec.LastRegisteredAt = msToTime(lastRegMs)
	rec.LastStartAt = msToTime(lastStartMs)
	rec.LastStopAt = msToTime(lastStopMs)
	rec.LastCrashAt = msToTime(lastCrashMs)

	return &rec, nil
}
