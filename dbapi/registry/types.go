package registry

import "time"

// PluginRecord is the merged read-side view across the declarative,
// authorization, and runtime layers of the registry.
type PluginRecord struct {
	// Declarative — derived from manifest.json by Discovery.
	PluginID        string
	DisplayName     string
	Version         string
	Description     string
	Endpoint        string
	Autostart       bool
	ManifestHash    string
	ProtocolVersion string
	Language        string

	// Authorization — operator-managed policy.
	Enabled                bool
	AuthorizedCapabilities []string
	AuthorizedStreams      []string
	AuthorizedCommands     []string
	AuthorizedQueries      []string

	// Runtime — observational summary.
	RuntimeState      string
	InstanceID        string
	SessionID         string
	RestartCount      int
	ActiveStreamCount int
	Degraded          bool
	LastErrorCode     string

	LastHeartbeat    *time.Time
	LastRegisteredAt *time.Time
	LastStartAt      *time.Time
	LastStopAt       *time.Time
	LastCrashAt      *time.Time
}

// LifecycleEvent is one row from plugin_lifecycle_events.
type LifecycleEvent struct {
	ID         int64
	PluginID   string
	InstanceID string
	EventType  string
	EventCode  string
	Summary    string
	CreatedAt  time.Time
}

// AuthRecord is the authentication-metadata view of one plugin.
type AuthRecord struct {
	PluginID              string
	AuthMode              string
	CredentialFingerprint string
	LastSuccessfulAuthAt  *time.Time
	LastRejectedAuthAt    *time.Time
	LastRejectionCode     string
	UpdatedAt             time.Time
}
