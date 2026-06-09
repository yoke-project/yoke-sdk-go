// Package logstore is the read-only Go client for the Yoke log store
// database. Open returns a Client that exposes typed query methods;
// the SQLite file is opened in mode=ro so callers can run concurrently
// with a writing yoke-core.
package logstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by Get* methods when no row matches.
var ErrNotFound = errors.New("dbapi/logstore: not found")

// Client is a read-only handle to a log store database file.
type Client struct {
	db *sql.DB
}

// Open opens path in read-only mode.
func Open(path string) (*Client, error) {
	conn, err := sql.Open("sqlite", path+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("dbapi/logstore: open %s: %w", path, err)
	}
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dbapi/logstore: ping %s: %w", path, err)
	}
	return &Client{db: conn}, nil
}

// Close releases the underlying database handle.
func (c *Client) Close() error { return c.db.Close() }

// Query returns log entries matching the filter, newest first.
func (c *Client) Query(ctx context.Context, f QueryFilter) ([]*Entry, error) {
	var (
		conds []string
		args  []any
	)
	if f.PluginID != "" {
		conds = append(conds, `plugin_id = ?`)
		args = append(args, f.PluginID)
	}
	if f.Source != "" {
		conds = append(conds, `source = ?`)
		args = append(args, f.Source)
	}
	if f.MinLevel != "" {
		conds = append(conds, `level_order >= ?`)
		args = append(args, levelOrder(f.MinLevel))
	}
	if !f.Since.IsZero() {
		conds = append(conds, `timestamp_unix_ms >= ?`)
		args = append(args, f.Since.UnixMilli())
	}
	if !f.Until.IsZero() {
		conds = append(conds, `timestamp_unix_ms < ?`)
		args = append(args, f.Until.UnixMilli())
	}
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}

	q := `
		SELECT id, timestamp_unix_ms, COALESCE(plugin_id, ''), source, level,
		       message, COALESCE(event_type, ''), COALESCE(raw_json, '')
		FROM log_entries`
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	q += ` ORDER BY timestamp_unix_ms DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dbapi/logstore: query: %w", err)
	}
	defer rows.Close()

	var out []*Entry
	for rows.Next() {
		var (
			e    Entry
			tsMs int64
		)
		if err := rows.Scan(&e.ID, &tsMs, &e.PluginID, &e.Source, &e.Level,
			&e.Message, &e.EventType, &e.RawJSON); err != nil {
			return nil, fmt.Errorf("dbapi/logstore: scan: %w", err)
		}
		e.Timestamp = time.UnixMilli(tsMs).UTC()
		out = append(out, &e)
	}
	return out, rows.Err()
}

// Count returns the total number of rows matching the filter (Limit
// is ignored). Useful for paging UIs.
func (c *Client) Count(ctx context.Context, f QueryFilter) (int64, error) {
	var (
		conds []string
		args  []any
	)
	if f.PluginID != "" {
		conds = append(conds, `plugin_id = ?`)
		args = append(args, f.PluginID)
	}
	if f.Source != "" {
		conds = append(conds, `source = ?`)
		args = append(args, f.Source)
	}
	if f.MinLevel != "" {
		conds = append(conds, `level_order >= ?`)
		args = append(args, levelOrder(f.MinLevel))
	}
	if !f.Since.IsZero() {
		conds = append(conds, `timestamp_unix_ms >= ?`)
		args = append(args, f.Since.UnixMilli())
	}
	if !f.Until.IsZero() {
		conds = append(conds, `timestamp_unix_ms < ?`)
		args = append(args, f.Until.UnixMilli())
	}
	q := `SELECT COUNT(*) FROM log_entries`
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	var n int64
	if err := c.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("dbapi/logstore: count: %w", err)
	}
	return n, nil
}

// ListRetentionPolicies returns every per-plugin retention override.
func (c *Client) ListRetentionPolicies(ctx context.Context) ([]*RetentionPolicy, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT plugin_id, max_age_hours, max_bytes, max_entries
		 FROM retention_policies ORDER BY plugin_id`)
	if err != nil {
		return nil, fmt.Errorf("dbapi/logstore: list retention: %w", err)
	}
	defer rows.Close()

	var out []*RetentionPolicy
	for rows.Next() {
		var (
			p          RetentionPolicy
			ageHours   sql.NullInt64
			maxBytes   sql.NullInt64
			maxEntries sql.NullInt64
		)
		if err := rows.Scan(&p.PluginID, &ageHours, &maxBytes, &maxEntries); err != nil {
			return nil, fmt.Errorf("dbapi/logstore: scan retention: %w", err)
		}
		if ageHours.Valid {
			v := int(ageHours.Int64)
			p.MaxAgeHours = &v
		}
		if maxBytes.Valid {
			v := maxBytes.Int64
			p.MaxBytes = &v
		}
		if maxEntries.Valid {
			v := maxEntries.Int64
			p.MaxEntries = &v
		}
		out = append(out, &p)
	}
	return out, rows.Err()
}

func levelOrder(level string) int {
	switch level {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	default:
		return 0
	}
}
