package coreapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/yoke-project/yoke-proto/gen/go/yoke/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/encoding/protojson"
)

// RegistrationRequest is the SDK-level view of an incoming registration attempt.
type RegistrationRequest struct {
	PluginID             string
	InstanceID           string
	PluginVersion        string
	ProtocolVersion      string
	Language             string
	SDKVersion           string
	BootstrapToken       string
	DeclaredCapabilities []string
	DeclaredStreams      []string
	DeclaredCommands     []string
	PID                  uint32
	Endpoint             string
	ManifestHash         string
	// MediaSocketPaths maps stream_id to the Unix socket path for media streams.
	// Empty for structured-only plugins.
	MediaSocketPaths map[string]string
}

// RegistrationResult is what the Core returns for a registration attempt.
type RegistrationResult struct {
	Accepted              bool
	RejectionCode         string
	RejectionMessage      string
	SessionID             string
	HeartbeatIntervalSecs uint32
	// Set when some declared items exceed policy; Core accepts with reduced set.
	RestrictedMode       bool
	RestrictionSummary   string
	Restrictions         []RegistrationRestriction
	AcceptedCapabilities []string
	AcceptedStreams      []string
	AcceptedCommands     []string
}

// RegistrationRestriction describes one restriction applied to an accepted session.
type RegistrationRestriction struct {
	Code   string
	Detail string
}

// SessionEvent is delivered to SessionHandler for session lifecycle events.
type SessionEvent struct {
	PluginID   string
	InstanceID string
	SessionID  string
	EventType  string // "open", "close", "heartbeat"
}

// HeartbeatInfo carries the plugin's health report.
type HeartbeatInfo struct {
	RuntimeState      string
	ActiveStreamCount uint32
	Degraded          bool
	Summary           string
}

// CommandInfo carries metadata about a command being dispatched Core → Plugin.
type CommandInfo struct {
	CommandID string
	// Type is one of: "StartStream", "StopStream", "Shutdown", "Restart",
	// "ReloadConfig", "GetStatus".
	Type     string
	StreamID string // set for StartStream / StopStream
}

// Command is the public representation of a Core-to-Plugin command.
// Use the New*Command helpers to construct values.
type Command struct {
	id       string
	cmdType  string
	streamID string
}

// NewShutdownCommand creates a Shutdown command.
func NewShutdownCommand() Command {
	return Command{id: newCmdID(), cmdType: "Shutdown"}
}

// NewStartStreamCommand creates a StartStream command for the given stream ID.
func NewStartStreamCommand(streamID string) Command {
	return Command{id: newCmdID(), cmdType: "StartStream", streamID: streamID}
}

// NewStopStreamCommand creates a StopStream command for the given stream ID.
func NewStopStreamCommand(streamID string) Command {
	return Command{id: newCmdID(), cmdType: "StopStream", streamID: streamID}
}

func newCmdID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// toPB converts a Command to the internal protobuf CommandMessage.
func (c Command) toPB() *pb.CommandMessage {
	msg := &pb.CommandMessage{CommandId: c.id}
	switch c.cmdType {
	case "Shutdown":
		msg.Command = &pb.CommandMessage_Shutdown{Shutdown: &pb.ShutdownCommand{}}
	case "StartStream":
		msg.Command = &pb.CommandMessage_StartStream{StartStream: &pb.StartStreamCommand{StreamId: c.streamID}}
	case "StopStream":
		msg.Command = &pb.CommandMessage_StopStream{StopStream: &pb.StopStreamCommand{StreamId: c.streamID}}
	case "Restart":
		msg.Command = &pb.CommandMessage_Restart{Restart: &pb.RestartCommand{}}
	case "ReloadConfig":
		msg.Command = &pb.CommandMessage_ReloadConfig{ReloadConfig: &pb.ReloadConfigCommand{}}
	case "GetStatus":
		msg.Command = &pb.CommandMessage_GetStatus{GetStatus: &pb.GetStatusCommand{}}
	}
	return msg
}

// RegistrationValidator is implemented by Core to validate registration attempts.
// Return a non-nil RegistrationResult with Accepted=false to reject.
type RegistrationValidator interface {
	Validate(ctx context.Context, req RegistrationRequest) RegistrationResult
}

// EventInfo carries a plugin-originated EventMessage forwarded to the log store.
type EventInfo struct {
	EventID   string
	EventType string
	Summary   string
	RawJSON   string // JSON-serialised payload, if present
}

// AckInfo carries the contents of a CommandAck received from a plugin.
type AckInfo struct {
	CommandID string
	AckType   string // "ACCEPTED", "COMPLETED", "REJECTED"
	Detail    string
}

// PluginErrorInfo carries the contents of an ErrorMessage received from a plugin.
type PluginErrorInfo struct {
	ErrorCode     string
	ErrorCategory string
	Summary       string
	Scope         string
	CorrelationID string
}

// QueryResult carries the result of a plugin query routed through Core.
type QueryResult struct {
	Success       bool
	ErrorCode     string
	SchemaFamily  string
	SchemaVersion string
	Payload       []byte
}

// DataInfo carries metadata and the raw payload from a DataMessage received from a plugin.
type DataInfo struct {
	StreamID      string
	SchemaFamily  string
	SchemaVersion string
	Sequence      uint64
	Payload       []byte
}

// SessionHandler is implemented by Core to handle session lifecycle events.
type SessionHandler interface {
	OnOpen(ctx context.Context, ev SessionEvent) error
	OnHeartbeat(ctx context.Context, ev SessionEvent, hb HeartbeatInfo)
	OnEvent(ctx context.Context, ev SessionEvent, info EventInfo)
	OnClose(ctx context.Context, ev SessionEvent)
	// OnAck is called when the plugin sends a CommandAck (ACCEPTED or COMPLETED).
	OnAck(ctx context.Context, ev SessionEvent, ack AckInfo)
	// OnPluginError is called when the plugin sends an ErrorMessage.
	OnPluginError(ctx context.Context, ev SessionEvent, info PluginErrorInfo)
	// OnData is called when the plugin sends a DataMessage.
	OnData(ctx context.Context, ev SessionEvent, d DataInfo)
	// OnHeartbeatMissed is called when the heartbeat watchdog fires because
	// no heartbeat was received within the expected window.
	OnHeartbeatMissed(ctx context.Context, ev SessionEvent)
	// OnCommandSent is called after a CommandMessage is successfully written
	// to the session stream toward the Plugin.
	OnCommandSent(ctx context.Context, ev SessionEvent, cmd CommandInfo)
}

// PluginServer binds RegistrationService + SessionService on a UDS socket.
type PluginServer struct {
	validator               RegistrationValidator
	handler                 SessionHandler
	grpcSrv                 *grpc.Server
	hbMult                  float64 // heartbeat timeout = interval × hbMult
	maxMissedBeforeShutdown int     // ShutdownCommand sent after this many consecutive misses (default 3)
	mu                      sync.RWMutex
	sessions                map[string]*acceptedSession
	pendingQueries          sync.Map // query_id → chan QueryResult
}

type acceptedSession struct {
	pluginID         string
	instanceID       string
	sessionID        string
	acceptedAt       time.Time
	hbTimeout        time.Duration
	commandCh        chan *pb.CommandMessage
	queryCh          chan *pb.SessionEnvelope // outbound QueryRequest envelopes
	mediaSocketPaths map[string]string        // stream_id → socket path; nil for structured-only plugins
}

// handlerTimeout is the maximum time allowed for a single SessionHandler callback.
// Handlers call into Core's registry (SQLite) which should complete well within
// this window; exceeding it signals a stuck subsystem.
const handlerTimeout = 5 * time.Second

// NewPluginServer creates a PluginServer with the given validator and handler.
// hbMult is the heartbeat timeout multiplier (e.g. 3.0 means 3× interval).
// Pass ≤0 to use the default of 3.0.
func NewPluginServer(validator RegistrationValidator, handler SessionHandler, hbMult float64) *PluginServer {
	if hbMult <= 0 {
		hbMult = 3.0
	}
	s := &PluginServer{
		validator:               validator,
		handler:                 handler,
		hbMult:                  hbMult,
		maxMissedBeforeShutdown: 3,
		sessions:                make(map[string]*acceptedSession),
	}
	s.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(4*1024*1024), // 4 MiB per message
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterRegistrationServiceServer(s.grpcSrv, &registrationSvcImpl{srv: s})
	pb.RegisterSessionServiceServer(s.grpcSrv, &sessionSvcImpl{srv: s})
	return s
}

// WithMaxMissedBeforeShutdown overrides the number of consecutive missed
// heartbeats that trigger an automatic ShutdownCommand. Default is 3.
func (s *PluginServer) WithMaxMissedBeforeShutdown(n int) *PluginServer {
	if n > 0 {
		s.maxMissedBeforeShutdown = n
	}
	return s
}

// ListenAndServe binds to socketPath and serves until ctx is done.
func (s *PluginServer) ListenAndServe(ctx context.Context, socketPath string) error {
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("coreapi: listen %s: %w", socketPath, err)
	}
	go func() {
		<-ctx.Done()
		s.grpcSrv.GracefulStop()
	}()
	return s.grpcSrv.Serve(lis)
}

// SendCommand dispatches cmd to the plugin identified by sessionID.
// Returns an error if the session is not found or the command channel is full.
func (s *PluginServer) SendCommand(sessionID string, cmd Command) error {
	s.mu.RLock()
	sess, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("coreapi: session %q not found", sessionID)
	}
	select {
	case sess.commandCh <- cmd.toPB():
		return nil
	default:
		return fmt.Errorf("coreapi: command channel full for session %q", sessionID)
	}
}

// SendCommandToPlugin is a convenience wrapper that looks up the session by
// pluginID (first active session found) and dispatches cmd.
func (s *PluginServer) SendCommandToPlugin(pluginID string, cmd Command) error {
	s.mu.RLock()
	var sess *acceptedSession
	for _, v := range s.sessions {
		if v.pluginID == pluginID {
			sess = v
			break
		}
	}
	s.mu.RUnlock()
	if sess == nil {
		return fmt.Errorf("coreapi: no active session for plugin %q", pluginID)
	}
	select {
	case sess.commandCh <- cmd.toPB():
		return nil
	default:
		return fmt.Errorf("coreapi: command channel full for plugin %q", pluginID)
	}
}

// GetMediaSocketPaths returns the media socket path map declared at registration
// time by the plugin with the given pluginID. Returns nil if the plugin has no
// active session or declared no media sockets.
func (s *PluginServer) GetMediaSocketPaths(pluginID string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sess := range s.sessions {
		if sess.pluginID == pluginID {
			return sess.mediaSocketPaths
		}
	}
	return nil
}

func (s *PluginServer) storeSession(sess *acceptedSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sess.sessionID] = sess
}

func (s *PluginServer) lookupSession(sessionID string) (*acceptedSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[sessionID]
	return sess, ok
}

func (s *PluginServer) deleteSession(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
}

// ActiveSession is a snapshot of a connected plugin session.
type ActiveSession struct {
	PluginID   string
	InstanceID string
	SessionID  string
	AcceptedAt time.Time
}

// ListSessions returns a snapshot of all currently open plugin sessions.
func (s *PluginServer) ListSessions() []ActiveSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ActiveSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, ActiveSession{
			PluginID:   sess.pluginID,
			InstanceID: sess.instanceID,
			SessionID:  sess.sessionID,
			AcceptedAt: sess.acceptedAt,
		})
	}
	return out
}

// --- RegistrationService impl ---

type registrationSvcImpl struct {
	pb.UnimplementedRegistrationServiceServer
	srv *PluginServer
}

func (r *registrationSvcImpl) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	sdkReq := RegistrationRequest{
		PluginID:             req.PluginId,
		InstanceID:           req.InstanceId,
		PluginVersion:        req.PluginVersion,
		ProtocolVersion:      req.ProtocolVersion,
		Language:             req.Language,
		SDKVersion:           req.SdkVersion,
		DeclaredCapabilities: req.DeclaredCapabilities,
		DeclaredStreams:      req.DeclaredStreams,
		DeclaredCommands:     req.DeclaredCommands,
	}
	if req.Auth != nil {
		if bt := req.Auth.GetBootstrapToken(); bt != nil {
			sdkReq.BootstrapToken = bt.Token
		}
	}
	if req.Runtime != nil {
		sdkReq.PID = req.Runtime.Pid
		sdkReq.Endpoint = req.Runtime.Endpoint
		sdkReq.ManifestHash = req.Runtime.ManifestHash
		sdkReq.MediaSocketPaths = req.Runtime.GetMediaSocketPaths()
	}

	result := r.srv.validator.Validate(ctx, sdkReq)

	if !result.Accepted {
		return &pb.RegisterResponse{
			Outcome:       pb.RegistrationOutcome_REGISTRATION_OUTCOME_REJECTED,
			ReasonCode:    result.RejectionCode,
			ReasonMessage: result.RejectionMessage,
		}, nil
	}

	hbIntervalSecs := result.HeartbeatIntervalSecs
	if hbIntervalSecs == 0 {
		hbIntervalSecs = 10
	}
	hbInterval := time.Duration(hbIntervalSecs) * time.Second
	hbTimeout := time.Duration(float64(hbInterval) * r.srv.hbMult)

	r.srv.storeSession(&acceptedSession{
		pluginID:         sdkReq.PluginID,
		instanceID:       sdkReq.InstanceID,
		sessionID:        result.SessionID,
		acceptedAt:       time.Now(),
		hbTimeout:        hbTimeout,
		commandCh:        make(chan *pb.CommandMessage, 16),
		queryCh:          make(chan *pb.SessionEnvelope, 16),
		mediaSocketPaths: sdkReq.MediaSocketPaths,
	})

	outcome := pb.RegistrationOutcome_REGISTRATION_OUTCOME_ACCEPTED
	reasonCode := ""
	if result.RestrictedMode {
		outcome = pb.RegistrationOutcome_REGISTRATION_OUTCOME_ACCEPTED_WITH_RESTRICTIONS
		reasonCode = "REGISTRATION.RESTRICTED_ACCEPTANCE_REQUIRED"
	}

	restrictions := make([]*pb.RegistrationRestriction, 0, len(result.Restrictions))
	for _, r := range result.Restrictions {
		restrictions = append(restrictions, &pb.RegistrationRestriction{
			RestrictionCode: r.Code,
			Detail:          r.Detail,
		})
	}

	log.Printf("coreapi: plugin %q registered  session=%s restricted=%v",
		sdkReq.PluginID, result.SessionID, result.RestrictedMode)

	return &pb.RegisterResponse{
		Outcome:      outcome,
		ReasonCode:   reasonCode,
		Restrictions: restrictions,
		Session: &pb.AcceptedSessionConfig{
			SessionId:                result.SessionID,
			HeartbeatIntervalSeconds: result.HeartbeatIntervalSecs,
			AcceptedCapabilities:     result.AcceptedCapabilities,
			AcceptedStreams:          result.AcceptedStreams,
			AcceptedCommands:         result.AcceptedCommands,
			RestrictedMode:           result.RestrictedMode,
			RestrictionSummary:       result.RestrictionSummary,
		},
	}, nil
}

// --- SessionService impl ---

type sessionSvcImpl struct {
	pb.UnimplementedSessionServiceServer
	srv *PluginServer
}

func (s *sessionSvcImpl) OpenSession(stream pb.SessionService_OpenSessionServer) error {
	ctx := stream.Context()

	// First envelope must be SessionMessage{type: OPEN}.
	env, err := stream.Recv()
	if err != nil {
		return err
	}

	sm := env.GetSession()
	if sm == nil || sm.Type != pb.SessionMessageType_SESSION_MESSAGE_TYPE_OPEN {
		return fmt.Errorf("coreapi: first envelope must be SessionOpen")
	}

	sess, ok := s.srv.lookupSession(env.SessionId)
	if !ok {
		return fmt.Errorf("coreapi: unknown session_id %q", env.SessionId)
	}

	ev := SessionEvent{
		PluginID:   sess.pluginID,
		InstanceID: sess.instanceID,
		SessionID:  sess.sessionID,
		EventType:  "open",
	}
	{
		hCtx, hCancel := context.WithTimeout(ctx, handlerTimeout)
		err := s.srv.handler.OnOpen(hCtx, ev)
		hCancel()
		if err != nil {
			return err
		}
	}

	log.Printf("coreapi: session open  plugin=%s session=%s", sess.pluginID, sess.sessionID)

	// Per-session context for the watchdog and sender goroutines.
	sessCtx, cancelSess := context.WithCancel(ctx)
	defer cancelSess()

	// Heartbeat reset channel: main loop sends a signal on each received heartbeat.
	hbResetCh := make(chan struct{}, 1)

	// Heartbeat watchdog goroutine.
	go s.runHeartbeatWatchdog(sessCtx, ev, sess.hbTimeout, hbResetCh,
		sess.commandCh, s.srv.maxMissedBeforeShutdown)

	// Command sender goroutine: drains commandCh and queryCh and writes to the stream.
	go s.runCommandSender(sessCtx, stream, ev, sess.commandCh, sess.queryCh)

	// Main receive loop.
	for {
		env, err := stream.Recv()
		if err != nil {
			closeEv := SessionEvent{
				PluginID:   sess.pluginID,
				InstanceID: sess.instanceID,
				SessionID:  sess.sessionID,
				EventType:  "close",
			}
			hCtx, hCancel := context.WithTimeout(ctx, handlerTimeout)
			s.srv.handler.OnClose(hCtx, closeEv)
			hCancel()
			s.srv.deleteSession(sess.sessionID)
			log.Printf("coreapi: session closed  plugin=%s session=%s", sess.pluginID, sess.sessionID)
			if err == io.EOF {
				return nil
			}
			return err
		}

		ev := SessionEvent{
			PluginID:   sess.pluginID,
			InstanceID: sess.instanceID,
			SessionID:  sess.sessionID,
		}

		hCtx, hCancel := context.WithTimeout(ctx, handlerTimeout)
		switch env.Body.(type) {
		case *pb.SessionEnvelope_Health:
			select {
			case hbResetCh <- struct{}{}:
			default:
			}
			hb := env.GetHealth()
			ev.EventType = "heartbeat"
			s.srv.handler.OnHeartbeat(hCtx, ev, HeartbeatInfo{
				RuntimeState:      hb.RuntimeState,
				ActiveStreamCount: hb.ActiveStreamCount,
				Degraded:          hb.Degraded,
				Summary:           hb.Summary,
			})

		case *pb.SessionEnvelope_Ack:
			a := env.GetAck()
			ev.EventType = "ack"
			s.srv.handler.OnAck(hCtx, ev, AckInfo{
				CommandID: a.CommandId,
				AckType:   a.AckType.String(),
				Detail:    a.Detail,
			})

		case *pb.SessionEnvelope_Error:
			e := env.GetError()
			ev.EventType = "error"
			s.srv.handler.OnPluginError(hCtx, ev, PluginErrorInfo{
				ErrorCode:     e.ErrorCode,
				ErrorCategory: e.ErrorCategory,
				Summary:       e.Summary,
				Scope:         e.Scope,
				CorrelationID: e.CorrelationId,
			})

		case *pb.SessionEnvelope_Data:
			dm := env.GetData()
			ev.EventType = "data"
			s.srv.handler.OnData(hCtx, ev, DataInfo{
				StreamID:      dm.StreamId,
				SchemaFamily:  dm.SchemaFamily,
				SchemaVersion: dm.SchemaVersion,
				Sequence:      dm.Sequence,
				Payload:       dm.Payload,
			})

		case *pb.SessionEnvelope_Event:
			em := env.GetEvent()
			rawJSON := marshalEventPayload(em)
			ev.EventType = em.EventType
			s.srv.handler.OnEvent(hCtx, ev, EventInfo{
				EventID:   em.EventId,
				EventType: em.EventType,
				Summary:   em.Summary,
				RawJSON:   rawJSON,
			})

		case *pb.SessionEnvelope_QueryResponse:
			qr := env.GetQueryResponse()
			if v, ok := s.srv.pendingQueries.LoadAndDelete(qr.QueryId); ok {
				resCh := v.(chan QueryResult)
				resCh <- QueryResult{
					Success:       qr.Success,
					ErrorCode:     qr.ErrorCode,
					SchemaFamily:  qr.SchemaFamily,
					SchemaVersion: qr.SchemaVersion,
					Payload:       qr.Payload,
				}
			}

		case *pb.SessionEnvelope_Session:
			sm := env.GetSession()
			if sm.Type == pb.SessionMessageType_SESSION_MESSAGE_TYPE_CLOSE {
				ev.EventType = "close"
				s.srv.handler.OnClose(hCtx, ev)
				hCancel()
				s.srv.deleteSession(sess.sessionID)
				log.Printf("coreapi: session closed by plugin  plugin=%s session=%s", sess.pluginID, sess.sessionID)
				return nil
			}
		}
		hCancel()
	}
}

// QueryPlugin sends a QueryRequest to the plugin identified by pluginID and
// waits for the corresponding QueryResponse. The context deadline controls the
// maximum wait time. Returns an error if the plugin has no active session,
// if the query channel is full, or if ctx expires before the response arrives.
func (s *PluginServer) QueryPlugin(ctx context.Context, pluginID, queryType string, params []byte) (QueryResult, error) {
	s.mu.RLock()
	var sess *acceptedSession
	for _, v := range s.sessions {
		if v.pluginID == pluginID {
			sess = v
			break
		}
	}
	s.mu.RUnlock()
	if sess == nil {
		return QueryResult{}, fmt.Errorf("coreapi: no active session for plugin %q", pluginID)
	}

	queryID := newCmdID()
	resCh := make(chan QueryResult, 1)
	s.pendingQueries.Store(queryID, resCh)

	env := &pb.SessionEnvelope{
		MessageId:       newCmdID(),
		PluginId:        sess.pluginID,
		InstanceId:      sess.instanceID,
		SessionId:       sess.sessionID,
		TimestampUnixMs: time.Now().UnixMilli(),
		Body: &pb.SessionEnvelope_QueryRequest{
			QueryRequest: &pb.QueryRequest{
				QueryId:   queryID,
				QueryType: queryType,
				Params:    params,
			},
		},
	}

	select {
	case sess.queryCh <- env:
	default:
		s.pendingQueries.Delete(queryID)
		return QueryResult{}, fmt.Errorf("coreapi: query channel full for plugin %q", pluginID)
	}

	select {
	case res := <-resCh:
		return res, nil
	case <-ctx.Done():
		s.pendingQueries.Delete(queryID)
		return QueryResult{}, fmt.Errorf("coreapi: query timeout for plugin %q: %w", pluginID, ctx.Err())
	}
}

// runHeartbeatWatchdog fires OnHeartbeatMissed when no heartbeat is received
// within timeout. After maxMisses consecutive misses it enqueues a ShutdownCommand.
func (s *sessionSvcImpl) runHeartbeatWatchdog(
	ctx context.Context,
	ev SessionEvent,
	timeout time.Duration,
	resetCh <-chan struct{},
	commandCh chan<- *pb.CommandMessage,
	maxMisses int,
) {
	if timeout <= 0 {
		return // watchdog disabled
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	missCount := 0
	for {
		select {
		case <-timer.C:
			missCount++
			s.srv.handler.OnHeartbeatMissed(ctx, ev)
			if maxMisses > 0 && missCount >= maxMisses {
				cmd := &pb.CommandMessage{
					CommandId: newCmdID(),
					Command:   &pb.CommandMessage_Shutdown{Shutdown: &pb.ShutdownCommand{}},
				}
				select {
				case commandCh <- cmd:
					log.Printf("coreapi: sustained degraded — ShutdownCommand sent  plugin=%s session=%s misses=%d",
						ev.PluginID, ev.SessionID, missCount)
				default:
				}
				return
			}
			timer.Reset(timeout)
		case <-resetCh:
			missCount = 0
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		case <-ctx.Done():
			return
		}
	}
}

// runCommandSender drains commandCh and queryCh and writes each envelope to the stream.
func (s *sessionSvcImpl) runCommandSender(
	ctx context.Context,
	stream pb.SessionService_OpenSessionServer,
	ev SessionEvent,
	commandCh <-chan *pb.CommandMessage,
	queryCh <-chan *pb.SessionEnvelope,
) {
	for {
		select {
		case cmd, ok := <-commandCh:
			if !ok {
				return
			}
			env := &pb.SessionEnvelope{
				PluginId:        ev.PluginID,
				InstanceId:      ev.InstanceID,
				SessionId:       ev.SessionID,
				TimestampUnixMs: time.Now().UnixMilli(),
			}
			env.Body = &pb.SessionEnvelope_Command{Command: cmd}
			if err := stream.Send(env); err != nil {
				log.Printf("coreapi: command send error  plugin=%s: %v", ev.PluginID, err)
				return
			}
			info := pbCommandToInfo(cmd)
			s.srv.handler.OnCommandSent(ctx, ev, info)
		case env, ok := <-queryCh:
			if !ok {
				return
			}
			if err := stream.Send(env); err != nil {
				log.Printf("coreapi: query send error  plugin=%s: %v", ev.PluginID, err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// pbCommandToInfo extracts a CommandInfo from a protobuf CommandMessage.
func pbCommandToInfo(cmd *pb.CommandMessage) CommandInfo {
	info := CommandInfo{CommandID: cmd.CommandId}
	switch c := cmd.Command.(type) {
	case *pb.CommandMessage_StartStream:
		info.Type = "StartStream"
		if c.StartStream != nil {
			info.StreamID = c.StartStream.StreamId
		}
	case *pb.CommandMessage_StopStream:
		info.Type = "StopStream"
		if c.StopStream != nil {
			info.StreamID = c.StopStream.StreamId
		}
	case *pb.CommandMessage_Shutdown:
		info.Type = "Shutdown"
	case *pb.CommandMessage_Restart:
		info.Type = "Restart"
	case *pb.CommandMessage_ReloadConfig:
		info.Type = "ReloadConfig"
	case *pb.CommandMessage_GetStatus:
		info.Type = "GetStatus"
	}
	return info
}

// marshalEventPayload serialises the EventMessage to JSON using protojson so that
// oneof fields and enum values are encoded correctly. Returns an empty string when
// the message has no payload or cannot be serialised.
func marshalEventPayload(em *pb.EventMessage) string {
	if em == nil || em.Payload == nil {
		return ""
	}
	b, err := protojson.Marshal(em)
	if err != nil {
		return ""
	}
	return string(b)
}
