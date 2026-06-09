package logstore

import "time"

// Source discriminator values.
const (
	SourceProcess     = "process"
	SourceCore        = "core"
	SourcePluginEvent = "plugin_event"
)

// Level severity values, in ascending order.
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// Entry is one row of log_entries.
type Entry struct {
	ID        int64
	Timestamp time.Time
	PluginID  string // empty for Core-level entries
	Source    string
	Level     string
	Message   string
	EventType string
	RawJSON   string
}

// QueryFilter parametrises Client.Query. Zero values disable the
// corresponding filter — an empty struct returns the most recent
// entries up to DefaultLimit.
type QueryFilter struct {
	PluginID string    // exact match; empty = any
	Source   string    // exact match; empty = any
	MinLevel string    // inclusive lower bound (LevelDebug…LevelError)
	Since    time.Time // inclusive; zero = no lower bound
	Until    time.Time // exclusive; zero = no upper bound
	Limit    int       // ≤ 0 → DefaultLimit
}

// DefaultLimit is the cap applied when QueryFilter.Limit is unset.
const DefaultLimit = 200

// RetentionPolicy is one row of retention_policies. PluginID "_core"
// governs core-level entries; a missing row means "use global default".
type RetentionPolicy struct {
	PluginID    string
	MaxAgeHours *int
	MaxBytes    *int64
	MaxEntries  *int64
}
