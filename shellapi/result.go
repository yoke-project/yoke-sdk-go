package shellapi

import "time"

// ResultKind identifies the type of ShellResult payload.
type ResultKind string

const (
	KindPluginList    ResultKind = "plugin_list"
	KindPluginStatus  ResultKind = "plugin_status"
	KindPluginInspect ResultKind = "plugin_inspect"
	KindPluginAction  ResultKind = "plugin_action"
	KindCoreStatus    ResultKind = "core_status"
	KindLogs          ResultKind = "logs"
	KindStreamAction  ResultKind = "stream_action"
	KindQueryResult   ResultKind = "query_result"
)

// ShellResult is the structured response returned by CommandHandler.Handle.
// Exactly one result field is non-nil, matching Kind.
type ShellResult struct {
	Kind ResultKind `json:"kind"`

	PluginList    *PluginListResult    `json:"plugin_list,omitempty"`
	PluginStatus  *PluginStatusResult  `json:"plugin_status,omitempty"`
	PluginInspect *PluginInspectResult `json:"plugin_inspect,omitempty"`
	PluginAction  *PluginActionResult  `json:"plugin_action,omitempty"`
	CoreStatus    *CoreStatusResult    `json:"core_status,omitempty"`
	Logs          *LogsResult          `json:"logs,omitempty"`
	StreamAction  *StreamActionResult  `json:"stream_action,omitempty"`
	QueryResult   *QueryResultPayload  `json:"query_result,omitempty"`
}

// QueryResultPayload is the response to "plugin query <plugin_id> <query_type>".
type QueryResultPayload struct {
	PluginID      string `json:"plugin_id"`
	QueryType     string `json:"query_type"`
	Success       bool   `json:"success"`
	ErrorCode     string `json:"error_code,omitempty"`
	SchemaFamily  string `json:"schema_family,omitempty"`
	SchemaVersion string `json:"schema_version,omitempty"`
	// Payload is the raw protobuf bytes returned by the plugin handler.
	// Consumers decode it using schema_family + schema_version.
	Payload []byte `json:"payload,omitempty"`
}

// PluginSessionRow is one entry in a plugin list.
type PluginSessionRow struct {
	PluginID   string    `json:"plugin_id"`
	SessionID  string    `json:"session_id"`
	AcceptedAt time.Time `json:"accepted_at"`
}

// PluginListResult is the response to "plugin list".
type PluginListResult struct {
	Sessions []PluginSessionRow `json:"sessions"`
}

// PluginStatusResult is the response to "plugin status <id>".
type PluginStatusResult struct {
	Found      bool      `json:"found"`
	PluginID   string    `json:"plugin_id,omitempty"`
	InstanceID string    `json:"instance_id,omitempty"`
	SessionID  string    `json:"session_id,omitempty"`
	AcceptedAt time.Time `json:"accepted_at,omitempty"`
	State      string    `json:"state,omitempty"`
}

// PluginInspectResult is the response to "plugin inspect <id>".
type PluginInspectResult struct {
	Found              bool     `json:"found"`
	PluginID           string   `json:"plugin_id,omitempty"`
	DisplayName        string   `json:"display_name,omitempty"`
	Version            string   `json:"version,omitempty"`
	Description        string   `json:"description,omitempty"`
	Language           string   `json:"language,omitempty"`
	Enabled            bool     `json:"enabled,omitempty"`
	State              string   `json:"state,omitempty"`
	AuthorizedStreams  []string `json:"authorized_streams,omitempty"`
	AuthorizedCommands []string `json:"authorized_commands,omitempty"`
	AuthorizedQueries  []string `json:"authorized_queries,omitempty"`
}

// PluginActionResult is the response to enable/disable/restart commands.
type PluginActionResult struct {
	PluginID string   `json:"plugin_id"`
	Action   string   `json:"action"`            // "enabled", "disabled", "restarted"
	Details  []string `json:"details,omitempty"` // optional extra info (e.g. "ShutdownCommand sent")
}

// CoreStatusResult is the response to "core status".
type CoreStatusResult struct {
	UptimeSeconds int64 `json:"uptime_seconds"`
	ActivePlugins int   `json:"active_plugins"`
}

// LogEntry is a single line in a log query result.
type LogEntry struct {
	TimestampUnixMS int64  `json:"timestamp_unix_ms"`
	Level           string `json:"level"`
	PluginID        string `json:"plugin_id"`
	Message         string `json:"message"`
}

// LogsResult is the response to "logs" queries.
type LogsResult struct {
	Entries    []LogEntry `json:"entries"`
	NextCursor int64      `json:"next_cursor,omitempty"`
}

// StreamActionResult is the response to stream start/stop/watch/unwatch.
type StreamActionResult struct {
	Action    string `json:"action"`     // "started", "stopped", "subscribed", "unsubscribed"
	StreamRef string `json:"stream_ref"` // "<plugin_id>/<stream_id>"
}
