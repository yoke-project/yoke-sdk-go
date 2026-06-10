// Package operatorapi implements the Core-side OperatorService gRPC server.
//
// Core provides a LogHandler and optionally a PluginManager implementation;
// this package wraps them in the generated gRPC server and binds it to operator.sock.
package operatorapi

import (
	"context"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	operatorpb "github.com/yoke-project/yoke-proto/gen/go/yoke/operator/v1"
)

// LogEntry is the SDK representation of a log record, independent of the
// generated protobuf type.
type LogEntry struct {
	ID              int64
	TimestampUnixMS int64
	PluginID        string
	Source          string
	Level           string
	Message         string
	EventType       string
	RawJSON         string
}

// RetentionPolicy holds global or per-plugin retention parameters.
type RetentionPolicy struct {
	MaxAgeHours int
	MaxBytes    int64
	MaxEntries  int64
}

// QueryFilter specifies constraints for a historical query.
type QueryFilter struct {
	PluginID    string
	Source      string
	MinLevel    string
	EventType   string
	SinceUnixMS int64
	UntilUnixMS int64
	Search      string
	AfterCursor int64
	PageSize    int
}

// QueryResult is the response to a historical log query.
type QueryResult struct {
	Entries    []LogEntry
	NextCursor int64
}

// TailFilter specifies constraints for a live tail stream.
type TailFilter struct {
	PluginID  string
	Source    string
	MinLevel  string
	EventType string
}

// TailRequest bundles filter and optional backfill for a Tail call.
type TailRequest struct {
	Filter        TailFilter
	BackfillLines int
}

// LogHandler is implemented by Core to service operator log requests.
type LogHandler interface {
	QueryLogs(ctx context.Context, f QueryFilter) (QueryResult, error)
	TailLogs(ctx context.Context, req TailRequest, send func(LogEntry) error) error
	GetRetentionPolicy(ctx context.Context, pluginID string) (RetentionPolicy, bool, error)
	SetRetentionPolicy(ctx context.Context, pluginID string, p RetentionPolicy, del bool) error
}

// PluginStatus is a snapshot of a plugin's administrative and runtime state.
type PluginStatus struct {
	PluginID               string
	AdminEnabled           bool
	RuntimeState           string
	SessionID              string
	RestartCount           int
	LastHeartbeatAtMS      int64
	AuthorizedCapabilities []string
}

// PluginSummary is a condensed single-entry view for ListPlugins.
type PluginSummary struct {
	PluginID     string
	AdminEnabled bool
	RuntimeState string
}

// CoreStatus holds a snapshot of overall Core health.
type CoreStatus struct {
	UptimeSeconds      int64
	Version            string
	LoadedPluginCount  int32
	ActiveSessionCount int32
}

// CapabilityChangeType indicates whether a capability should be added or removed.
type CapabilityChangeType int

const (
	CapabilityChangeAdd    CapabilityChangeType = 1
	CapabilityChangeRemove CapabilityChangeType = 2
)

// CapabilityChange describes one modification to a plugin's capability policy.
type CapabilityChange struct {
	Type       CapabilityChangeType
	Capability string
}

// PluginManager is implemented by Core to service operator management requests.
type PluginManager interface {
	GetPluginStatus(ctx context.Context, pluginID string) (PluginStatus, error)
	ListPlugins(ctx context.Context) ([]PluginSummary, error)
	GetCoreStatus(ctx context.Context) (CoreStatus, error)
	EnablePlugin(ctx context.Context, pluginID string) (wasEnabled bool, err error)
	DisablePlugin(ctx context.Context, pluginID string) (shutdownInitiated bool, err error)
	ForceRestart(ctx context.Context, pluginID string) (shutdownInitiated bool, launchInitiated bool, err error)
	UpdatePolicy(ctx context.Context, pluginID string, changes []CapabilityChange) error
}

// Server wraps a LogHandler (and optionally a PluginManager) as a gRPC OperatorService.
type Server struct {
	operatorpb.UnimplementedOperatorServiceServer
	handler LogHandler
	mgr     PluginManager // may be nil
}

// NewServer returns a Server backed by handler.
func NewServer(handler LogHandler) *Server {
	return &Server{handler: handler}
}

// WithPluginManager attaches a PluginManager to the server.
// Returns s for chaining.
func (s *Server) WithPluginManager(mgr PluginManager) *Server {
	s.mgr = mgr
	return s
}

// ListenAndServe binds the gRPC server to sockPath and serves until ctx is
// cancelled or the server fails.
func (s *Server) ListenAndServe(ctx context.Context, sockPath string) error {
	_ = os.Remove(sockPath)
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	operatorpb.RegisterOperatorServiceServer(srv, s)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	return srv.Serve(lis)
}

// QueryLogs serves a paginated historical log query.
func (s *Server) QueryLogs(ctx context.Context, req *operatorpb.QueryLogsRequest) (*operatorpb.QueryLogsResponse, error) {
	res, err := s.handler.QueryLogs(ctx, QueryFilter{
		PluginID:    req.PluginId,
		Source:      req.Source,
		MinLevel:    req.MinLevel,
		EventType:   req.EventType,
		SinceUnixMS: req.SinceUnixMs,
		UntilUnixMS: req.UntilUnixMs,
		Search:      req.Search,
		AfterCursor: req.AfterCursor,
		PageSize:    int(req.PageSize),
	})
	if err != nil {
		return nil, err
	}
	resp := &operatorpb.QueryLogsResponse{
		NextCursor: res.NextCursor,
		Entries:    make([]*operatorpb.LogEntry, len(res.Entries)),
	}
	for i, e := range res.Entries {
		resp.Entries[i] = entryToProto(e)
	}
	return resp, nil
}

// TailLogs streams live log entries.
func (s *Server) TailLogs(req *operatorpb.TailLogsRequest, stream operatorpb.OperatorService_TailLogsServer) error {
	return s.handler.TailLogs(stream.Context(), TailRequest{
		Filter: TailFilter{
			PluginID:  req.PluginId,
			Source:    req.Source,
			MinLevel:  req.MinLevel,
			EventType: req.EventType,
		},
		BackfillLines: int(req.BackfillLines),
	}, func(e LogEntry) error {
		return stream.Send(&operatorpb.TailLogsResponse{Entry: entryToProto(e)})
	})
}

// GetRetentionPolicy returns the effective retention policy for a plugin.
func (s *Server) GetRetentionPolicy(ctx context.Context, req *operatorpb.GetRetentionPolicyRequest) (*operatorpb.GetRetentionPolicyResponse, error) {
	p, isOverride, err := s.handler.GetRetentionPolicy(ctx, req.PluginId)
	if err != nil {
		return nil, err
	}
	return &operatorpb.GetRetentionPolicyResponse{
		Policy: &operatorpb.RetentionPolicy{
			MaxAgeHours: int32(p.MaxAgeHours),
			MaxBytes:    p.MaxBytes,
			MaxEntries:  p.MaxEntries,
		},
		IsOverride: isOverride,
	}, nil
}

// UpdateRetentionPolicy upserts or deletes a per-plugin retention override.
func (s *Server) UpdateRetentionPolicy(ctx context.Context, req *operatorpb.UpdateRetentionPolicyRequest) (*operatorpb.UpdateRetentionPolicyResponse, error) {
	var p RetentionPolicy
	if req.Policy != nil {
		p = RetentionPolicy{
			MaxAgeHours: int(req.Policy.MaxAgeHours),
			MaxBytes:    req.Policy.MaxBytes,
			MaxEntries:  req.Policy.MaxEntries,
		}
	}
	if err := s.handler.SetRetentionPolicy(ctx, req.PluginId, p, req.Delete); err != nil {
		return nil, err
	}
	return &operatorpb.UpdateRetentionPolicyResponse{}, nil
}

// GetPluginStatus returns the administrative and runtime status of a plugin.
func (s *Server) GetPluginStatus(ctx context.Context, req *operatorpb.GetPluginStatusRequest) (*operatorpb.GetPluginStatusResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	ps, err := s.mgr.GetPluginStatus(ctx, req.PluginId)
	if err != nil {
		return nil, err
	}
	return &operatorpb.GetPluginStatusResponse{
		Status: &operatorpb.PluginStatus{
			PluginId:               ps.PluginID,
			AdminEnabled:           ps.AdminEnabled,
			RuntimeState:           ps.RuntimeState,
			SessionId:              ps.SessionID,
			RestartCount:           int32(ps.RestartCount),
			LastHeartbeatAtMs:      ps.LastHeartbeatAtMS,
			AuthorizedCapabilities: ps.AuthorizedCapabilities,
		},
	}, nil
}

// ListPlugins returns a summary list of all registered plugins.
func (s *Server) ListPlugins(ctx context.Context, req *operatorpb.ListPluginsRequest) (*operatorpb.ListPluginsResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	entries, err := s.mgr.ListPlugins(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*operatorpb.PluginSummary, len(entries))
	for i, e := range entries {
		out[i] = &operatorpb.PluginSummary{
			PluginId:     e.PluginID,
			AdminEnabled: e.AdminEnabled,
			RuntimeState: e.RuntimeState,
		}
	}
	return &operatorpb.ListPluginsResponse{Entries: out}, nil
}

// GetCoreStatus returns a snapshot of the overall Core status.
func (s *Server) GetCoreStatus(ctx context.Context, req *operatorpb.GetCoreStatusRequest) (*operatorpb.GetCoreStatusResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	cs, err := s.mgr.GetCoreStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &operatorpb.GetCoreStatusResponse{
		UptimeSeconds:      cs.UptimeSeconds,
		Version:            cs.Version,
		LoadedPluginCount:  cs.LoadedPluginCount,
		ActiveSessionCount: cs.ActiveSessionCount,
	}, nil
}

// EnablePlugin sets the plugin's administrative state to enabled.
func (s *Server) EnablePlugin(ctx context.Context, req *operatorpb.EnablePluginRequest) (*operatorpb.EnablePluginResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	wasEnabled, err := s.mgr.EnablePlugin(ctx, req.PluginId)
	if err != nil {
		return nil, err
	}
	prev := "disabled"
	if wasEnabled {
		prev = "enabled"
	}
	return &operatorpb.EnablePluginResponse{Success: true, PreviousState: prev}, nil
}

// DisablePlugin sets the plugin's administrative state to disabled and sends ShutdownCommand.
func (s *Server) DisablePlugin(ctx context.Context, req *operatorpb.DisablePluginRequest) (*operatorpb.DisablePluginResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	shutdownInitiated, err := s.mgr.DisablePlugin(ctx, req.PluginId)
	if err != nil {
		return nil, err
	}
	return &operatorpb.DisablePluginResponse{Success: true, ShutdownInitiated: shutdownInitiated}, nil
}

// ForceRestart sends ShutdownCommand to the active instance and requests a supervisor relaunch.
func (s *Server) ForceRestart(ctx context.Context, req *operatorpb.ForceRestartRequest) (*operatorpb.ForceRestartResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	shutdownSent, launchSent, err := s.mgr.ForceRestart(ctx, req.PluginId)
	if err != nil {
		return nil, err
	}
	return &operatorpb.ForceRestartResponse{
		Success:           true,
		ShutdownInitiated: shutdownSent,
		LaunchInitiated:   launchSent,
	}, nil
}

// UpdatePolicy modifies the capability authorization profile for a plugin.
func (s *Server) UpdatePolicy(ctx context.Context, req *operatorpb.UpdatePolicyRequest) (*operatorpb.UpdatePolicyResponse, error) {
	if s.mgr == nil {
		return nil, status.Error(codes.Unimplemented, "plugin manager not configured")
	}
	changes := make([]CapabilityChange, len(req.CapabilityChanges))
	for i, c := range req.CapabilityChanges {
		ct := CapabilityChangeAdd
		if c.ChangeType == operatorpb.CapabilityChangeType_CAPABILITY_CHANGE_TYPE_REMOVE {
			ct = CapabilityChangeRemove
		}
		changes[i] = CapabilityChange{Type: ct, Capability: c.Capability}
	}
	if err := s.mgr.UpdatePolicy(ctx, req.PluginId, changes); err != nil {
		return nil, err
	}
	return &operatorpb.UpdatePolicyResponse{Success: true, EffectiveAt: "next_registration"}, nil
}

func entryToProto(e LogEntry) *operatorpb.LogEntry {
	return &operatorpb.LogEntry{
		Id:              e.ID,
		TimestampUnixMs: e.TimestampUnixMS,
		PluginId:        e.PluginID,
		Source:          e.Source,
		Level:           e.Level,
		Message:         e.Message,
		EventType:       e.EventType,
		RawJson:         e.RawJSON,
	}
}
