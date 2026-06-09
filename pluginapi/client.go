package pluginapi

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/yoke-project/yoke-proto/gen/go/yoke/plugin/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// IncomingCommand is a command dispatched by Core to this plugin.
type IncomingCommand struct {
	CommandID string
	// Type is one of: "StartStream", "StopStream", "Shutdown", "Restart",
	// "ReloadConfig", "GetStatus", "Unknown".
	Type     string
	StreamID string // populated for StartStream / StopStream
}

// RegistrationConfig carries the fields a plugin provides at registration time.
type RegistrationConfig struct {
	PluginID        string
	InstanceID      string // leave empty to auto-generate
	PluginVersion   string
	ProtocolVersion string
	Language        string
	SDKVersion      string
	BootstrapToken  string
	Capabilities    []string
	Streams         []string
	Commands        []string
	PID             uint32
	Endpoint        string
	ManifestHash    string
	// MediaSocketPaths maps stream_id to the Unix socket path for media streams.
	// Must be populated (and sockets created) before calling Register.
	// Leave nil or empty for structured-only plugins.
	MediaSocketPaths map[string]string
}

// QueryHandler responds to a single QueryRequest from Core.
// It must be registered before OpenSession.
// schemaFamily and schemaVersion identify the response payload; return an empty
// payload and a non-empty errorCode to signal failure.
type QueryHandler func(ctx context.Context, queryType string, params []byte) (schemaFamily, schemaVersion string, payload []byte, errorCode string)

// registeredProducer holds state for a StreamProducer registered with the SDK.
type registeredProducer struct {
	producer      StreamProducer
	schemaFamily  string
	schemaVersion string
	cancel        context.CancelFunc // nil when idle
	// emission statistics (atomic access)
	bytesEmitted  uint64
	framesEmitted uint64
	framesDropped uint64
	lastEmitAt    int64 // unix nano
	streaming     int32 // 0=idle 1=streaming (atomic)
}

// Client handles the full plugin protocol lifecycle: registration, session, heartbeat.
type Client struct {
	cfg               RegistrationConfig
	conn              *grpc.ClientConn
	regSvc            pb.RegistrationServiceClient
	sessSvc           pb.SessionServiceClient
	stream            pb.SessionService_OpenSessionClient
	sendMu            sync.Mutex // guards all c.stream.Send calls
	commandCh         chan IncomingCommand
	producersMu       sync.Mutex
	producers         map[string]*registeredProducer // stream_id → producer
	handlersMu        sync.Mutex
	handlers          map[string]QueryHandler // query_type → handler
	SessionID         string
	HeartbeatInterval time.Duration
	// Set on ACCEPTED_WITH_RESTRICTIONS; false on plain ACCEPTED.
	RestrictedMode       bool
	RestrictionSummary   string
	AcceptedCapabilities []string
	AcceptedStreams      []string
	AcceptedCommands     []string
}

// Dial connects to core.sock and returns a ready-to-register Client.
func Dial(ctx context.Context, socketPath string, cfg RegistrationConfig) (*Client, error) {
	if cfg.InstanceID == "" {
		cfg.InstanceID = newInstanceID()
	}
	if cfg.ProtocolVersion == "" {
		cfg.ProtocolVersion = "1.0"
	}
	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("pluginapi: dial %s: %w", socketPath, err)
	}
	return &Client{
		cfg:       cfg,
		conn:      conn,
		regSvc:    pb.NewRegistrationServiceClient(conn),
		sessSvc:   pb.NewSessionServiceClient(conn),
		commandCh: make(chan IncomingCommand, 64),
		producers: make(map[string]*registeredProducer),
		handlers:  make(map[string]QueryHandler),
	}, nil
}

// registerTimeout is the maximum duration allowed for the Register unary RPC.
// It guards against Core being unreachable or unresponsive at plugin startup.
const registerTimeout = 30 * time.Second

// Register sends a RegisterRequest and processes the response.
// On success it sets c.SessionID and c.HeartbeatInterval.
func (c *Client) Register(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	req := &pb.RegisterRequest{
		PluginId:        c.cfg.PluginID,
		InstanceId:      c.cfg.InstanceID,
		PluginVersion:   c.cfg.PluginVersion,
		ProtocolVersion: c.cfg.ProtocolVersion,
		Language:        c.cfg.Language,
		SdkVersion:      c.cfg.SDKVersion,
		Auth: &pb.AuthMaterial{
			Method: &pb.AuthMaterial_BootstrapToken{
				BootstrapToken: &pb.BootstrapToken{Token: c.cfg.BootstrapToken},
			},
		},
		DeclaredCapabilities: c.cfg.Capabilities,
		DeclaredStreams:      c.cfg.Streams,
		DeclaredCommands:     c.cfg.Commands,
		Runtime: &pb.RuntimeMetadata{
			Pid:              c.cfg.PID,
			Endpoint:         c.cfg.Endpoint,
			ManifestHash:     c.cfg.ManifestHash,
			StartupUnixMs:    time.Now().UnixMilli(),
			MediaSocketPaths: c.cfg.MediaSocketPaths,
		},
	}

	resp, err := c.regSvc.Register(ctx, req)
	if err != nil {
		return fmt.Errorf("pluginapi: register RPC: %w", err)
	}

	switch resp.Outcome {
	case pb.RegistrationOutcome_REGISTRATION_OUTCOME_ACCEPTED,
		pb.RegistrationOutcome_REGISTRATION_OUTCOME_ACCEPTED_WITH_RESTRICTIONS:
		sess := resp.Session
		c.SessionID = sess.SessionId
		secs := sess.HeartbeatIntervalSeconds
		if secs == 0 {
			secs = 10
		}
		c.HeartbeatInterval = time.Duration(secs) * time.Second
		c.RestrictedMode = sess.RestrictedMode
		c.RestrictionSummary = sess.RestrictionSummary
		c.AcceptedCapabilities = sess.AcceptedCapabilities
		c.AcceptedStreams = sess.AcceptedStreams
		c.AcceptedCommands = sess.AcceptedCommands
		log.Printf("pluginapi: registered  plugin=%s session=%s heartbeat=%s restricted=%v",
			c.cfg.PluginID, c.SessionID, c.HeartbeatInterval, c.RestrictedMode)
		return nil
	default:
		return fmt.Errorf("pluginapi: registration rejected: [%s] %s",
			resp.ReasonCode, resp.ReasonMessage)
	}
}

// OpenSession opens the bidirectional session stream and sends SessionOpen.
func (c *Client) OpenSession(ctx context.Context) error {
	var err error
	c.stream, err = c.sessSvc.OpenSession(ctx)
	if err != nil {
		return fmt.Errorf("pluginapi: open session stream: %w", err)
	}

	env := c.newEnvelope()
	env.Body = &pb.SessionEnvelope_Session{
		Session: &pb.SessionMessage{Type: pb.SessionMessageType_SESSION_MESSAGE_TYPE_OPEN},
	}
	if err := c.sendEnvelope(env); err != nil {
		return fmt.Errorf("pluginapi: send session open: %w", err)
	}

	log.Printf("pluginapi: session opened  plugin=%s session=%s", c.cfg.PluginID, c.SessionID)

	go c.runReceiveLoop(ctx)
	return nil
}

// Commands returns the channel on which Core-dispatched commands are delivered.
// The channel is closed when the session ends.
func (c *Client) Commands() <-chan IncomingCommand {
	return c.commandCh
}

// runReceiveLoop drains server-sent envelopes and dispatches commands.
// Commands targeting a registered StreamProducer are handled automatically;
// all others are forwarded to commandCh.
func (c *Client) runReceiveLoop(ctx context.Context) {
	defer close(c.commandCh)
	for {
		env, err := c.stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("pluginapi: receive error  plugin=%s: %v", c.cfg.PluginID, err)
			}
			return
		}
		switch env.Body.(type) {
		case *pb.SessionEnvelope_Command:
			cmd := env.GetCommand()
			incoming := pbCommandToIncoming(cmd)
			if c.dispatchToProducer(ctx, incoming) {
				continue // handled by StreamProducer
			}
			select {
			case c.commandCh <- incoming:
			case <-ctx.Done():
				return
			}
		case *pb.SessionEnvelope_QueryRequest:
			go c.dispatchQueryHandler(ctx, env.GetQueryRequest())
		case *pb.SessionEnvelope_Session:
			sm := env.GetSession()
			if sm.Type == pb.SessionMessageType_SESSION_MESSAGE_TYPE_REVOKED {
				log.Printf("pluginapi: session revoked  plugin=%s session=%s", c.cfg.PluginID, c.SessionID)
				return
			}
		}
	}
}

// dispatchQueryHandler executes the registered handler for the query type and
// sends a QueryResponse back to Core on the session stream.
func (c *Client) dispatchQueryHandler(ctx context.Context, qr *pb.QueryRequest) {
	c.handlersMu.Lock()
	h, ok := c.handlers[qr.QueryType]
	c.handlersMu.Unlock()

	var resp *pb.QueryResponse
	if !ok {
		resp = &pb.QueryResponse{
			QueryId:   qr.QueryId,
			Success:   false,
			ErrorCode: "QUERY.HANDLER_NOT_FOUND",
		}
	} else {
		sf, sv, payload, errCode := h(ctx, qr.QueryType, qr.Params)
		success := errCode == ""
		resp = &pb.QueryResponse{
			QueryId:       qr.QueryId,
			Success:       success,
			ErrorCode:     errCode,
			SchemaFamily:  sf,
			SchemaVersion: sv,
			Payload:       payload,
		}
	}

	env := c.newEnvelope()
	env.Body = &pb.SessionEnvelope_QueryResponse{QueryResponse: resp}
	if err := c.sendEnvelope(env); err != nil {
		log.Printf("pluginapi: send query response  plugin=%s query_id=%s: %v",
			c.cfg.PluginID, qr.QueryId, err)
	}
}

// dispatchToProducer handles StartStream/StopStream for registered producers.
// Returns true if the command was consumed (should not be forwarded to commandCh).
func (c *Client) dispatchToProducer(ctx context.Context, inc IncomingCommand) bool {
	switch inc.Type {
	case "StartStream":
		c.producersMu.Lock()
		rp, ok := c.producers[inc.StreamID]
		c.producersMu.Unlock()
		if !ok {
			return false
		}
		go c.runProducer(ctx, inc.StreamID, inc.CommandID, rp)
		return true

	case "StopStream":
		c.producersMu.Lock()
		rp, ok := c.producers[inc.StreamID]
		c.producersMu.Unlock()
		if !ok {
			return false
		}
		c.stopProducer(ctx, inc.CommandID, rp)
		return true
	}
	return false
}

// RegisterStreamProducer registers a StreamProducer for the given stream.
// schemaFamily and schemaVersion identify the payload schema for structured streams;
// leave empty for media streams.
// Must be called before OpenSession.
func (c *Client) RegisterStreamProducer(streamID, schemaFamily, schemaVersion string, p StreamProducer) {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()
	c.producers[streamID] = &registeredProducer{
		producer:      p,
		schemaFamily:  schemaFamily,
		schemaVersion: schemaVersion,
	}
}

// RegisterQueryHandler registers a QueryHandler for the given query_type.
// Must be called before OpenSession.
func (c *Client) RegisterQueryHandler(queryType string, h QueryHandler) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	c.handlers[queryType] = h
}

// ActiveStreamCount returns the number of currently streaming producers.
// Useful for constructing the Heartbeat activeStreams field.
func (c *Client) ActiveStreamCount() uint32 {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()
	var n uint32
	for _, rp := range c.producers {
		if atomic.LoadInt32(&rp.streaming) == 1 {
			n++
		}
	}
	return n
}

// runProducer starts a StreamProducer's OnStart in the calling goroutine.
func (c *Client) runProducer(ctx context.Context, streamID, cmdID string, rp *registeredProducer) {
	prodCtx, cancel := context.WithCancel(ctx)

	c.producersMu.Lock()
	rp.cancel = cancel
	c.producersMu.Unlock()

	atomic.StoreInt32(&rp.streaming, 1)
	_ = c.SendAck(ctx, cmdID, pb.AckType_ACK_TYPE_ACCEPTED, "")

	emitter := c.buildEmitter(prodCtx, streamID, rp)
	if rp.producer.OnStart != nil {
		rp.producer.OnStart(prodCtx, emitter)
	}

	atomic.StoreInt32(&rp.streaming, 0)
	_ = c.SendAck(ctx, cmdID, pb.AckType_ACK_TYPE_COMPLETED, "")
}

func (c *Client) stopProducer(ctx context.Context, cmdID string, rp *registeredProducer) {
	c.producersMu.Lock()
	cancel := rp.cancel
	rp.cancel = nil
	c.producersMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rp.producer.OnStop != nil {
		rp.producer.OnStop()
	}
	_ = c.SendAck(ctx, cmdID, pb.AckType_ACK_TYPE_ACCEPTED, "")
}

// buildEmitter constructs the appropriate StreamEmitter for the given stream.
// Media streams (present in MediaSocketPaths) get a mediaEmitter;
// structured streams get a structuredEmitter.
func (c *Client) buildEmitter(ctx context.Context, streamID string, rp *registeredProducer) StreamEmitter {
	if socketPath, ok := c.cfg.MediaSocketPaths[streamID]; ok {
		return newMediaEmitter(ctx, socketPath, rp)
	}
	return &structuredEmitter{
		client:        c,
		ctx:           ctx,
		streamID:      streamID,
		schemaFamily:  rp.schemaFamily,
		schemaVersion: rp.schemaVersion,
		rp:            rp,
	}
}

// ── structuredEmitter ────────────────────────────────────────────────────────

type structuredEmitter struct {
	client        *Client
	ctx           context.Context
	streamID      string
	schemaFamily  string
	schemaVersion string
	seq           uint64 // accessed atomically
	rp            *registeredProducer
}

func (e *structuredEmitter) EmitData(payload []byte) error {
	seq := atomic.AddUint64(&e.seq, 1) - 1
	err := e.client.SendDataBytes(e.ctx, e.streamID, e.schemaFamily, e.schemaVersion, seq, payload)
	if err == nil {
		atomic.AddUint64(&e.rp.bytesEmitted, uint64(len(payload)))
		atomic.AddUint64(&e.rp.framesEmitted, 1)
		atomic.StoreInt64(&e.rp.lastEmitAt, time.Now().UnixNano())
	}
	return err
}

func (e *structuredEmitter) EmitFrame(FrameType, int64, []byte) error {
	return errors.New("pluginapi: EmitFrame not supported on structured streams")
}

// ── mediaEmitter ─────────────────────────────────────────────────────────────

type mediaEmitter struct {
	ln   net.Listener
	conn net.Conn // first accepted Core connection; nil until Core connects
	seq  uint32   // accessed atomically
	rp   *registeredProducer
}

func newMediaEmitter(ctx context.Context, socketPath string, rp *registeredProducer) *mediaEmitter {
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Printf("pluginapi: media socket listen %s: %v", socketPath, err)
		return &mediaEmitter{rp: rp}
	}
	e := &mediaEmitter{ln: ln, rp: rp}
	go e.acceptLoop(ctx)
	return e
}

func (e *mediaEmitter) acceptLoop(ctx context.Context) {
	if e.ln == nil {
		return
	}
	defer e.ln.Close()
	go func() {
		<-ctx.Done()
		_ = e.ln.Close()
	}()
	for {
		conn, err := e.ln.Accept()
		if err != nil {
			return
		}
		e.conn = conn // last accepted connection wins (single-consumer model for now)
	}
}

func (e *mediaEmitter) EmitFrame(ft FrameType, ptsNs int64, payload []byte) error {
	if e.conn == nil {
		return nil // no consumer yet; drop silently
	}
	hdr := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = 1 // format_version
	hdr[5] = uint8(ft)
	binary.LittleEndian.PutUint64(hdr[6:14], uint64(ptsNs))
	binary.LittleEndian.PutUint32(hdr[14:18], atomic.AddUint32(&e.seq, 1)-1)

	frame := make([]byte, frameHeaderSize+len(payload))
	copy(frame, hdr)
	copy(frame[frameHeaderSize:], payload)

	_, err := e.conn.Write(frame)
	if err == nil {
		atomic.AddUint64(&e.rp.bytesEmitted, uint64(len(frame)))
		atomic.AddUint64(&e.rp.framesEmitted, 1)
		atomic.StoreInt64(&e.rp.lastEmitAt, time.Now().UnixNano())
	}
	return err
}

func (e *mediaEmitter) EmitData([]byte) error {
	return errors.New("pluginapi: EmitData not supported on media streams")
}

const frameHeaderSize = 18

func pbCommandToIncoming(cmd *pb.CommandMessage) IncomingCommand {
	inc := IncomingCommand{CommandID: cmd.CommandId}
	switch c := cmd.Command.(type) {
	case *pb.CommandMessage_StartStream:
		inc.Type = "StartStream"
		if c.StartStream != nil {
			inc.StreamID = c.StartStream.StreamId
		}
	case *pb.CommandMessage_StopStream:
		inc.Type = "StopStream"
		if c.StopStream != nil {
			inc.StreamID = c.StopStream.StreamId
		}
	case *pb.CommandMessage_Shutdown:
		inc.Type = "Shutdown"
	case *pb.CommandMessage_Restart:
		inc.Type = "Restart"
	case *pb.CommandMessage_ReloadConfig:
		inc.Type = "ReloadConfig"
	case *pb.CommandMessage_GetStatus:
		inc.Type = "GetStatus"
	default:
		inc.Type = "Unknown"
	}
	return inc
}

// SendHeartbeat emits one Heartbeat envelope on the active session stream.
func (c *Client) SendHeartbeat(ctx context.Context, runtimeState string, activeStreams uint32, degraded bool) error {
	env := c.newEnvelope()
	env.Body = &pb.SessionEnvelope_Health{
		Health: &pb.Heartbeat{
			RuntimeState:      runtimeState,
			ActiveStreamCount: activeStreams,
			Degraded:          degraded,
		},
	}
	if err := c.sendEnvelope(env); err != nil {
		return fmt.Errorf("pluginapi: send heartbeat: %w", err)
	}
	return nil
}

// RunHeartbeat blocks, sending heartbeats at c.HeartbeatInterval until ctx is cancelled.
func (c *Client) RunHeartbeat(ctx context.Context, stateFunc func() (state string, activeStreams uint32, degraded bool)) error {
	ticker := time.NewTicker(c.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			state, streams, deg := stateFunc()
			if err := c.SendHeartbeat(ctx, state, streams, deg); err != nil {
				return err
			}
		}
	}
}

// SendAck sends a CommandAck envelope for the given command.
// Use pb.AckType_ACK_TYPE_ACCEPTED immediately on receipt and
// pb.AckType_ACK_TYPE_COMPLETED once execution finishes.
func (c *Client) SendAck(ctx context.Context, commandID string, ackType pb.AckType, detail string) error {
	env := c.newEnvelope()
	env.Body = &pb.SessionEnvelope_Ack{
		Ack: &pb.CommandAck{
			CommandId: commandID,
			AckType:   ackType,
			Detail:    detail,
		},
	}
	if err := c.sendEnvelope(env); err != nil {
		return fmt.Errorf("pluginapi: send ack: %w", err)
	}
	return nil
}

// SendDataBytes emits a DataMessage with an opaque bytes payload.
// The plugin serializes its own proto message to payload using proto.Marshal
// and identifies the schema via schemaFamily + schemaVersion.
// Core transports the bytes without interpreting them.
func (c *Client) SendDataBytes(ctx context.Context, streamID, schemaFamily, schemaVersion string, seq uint64, payload []byte) error {
	dm := &pb.DataMessage{
		StreamId:      streamID,
		SchemaFamily:  schemaFamily,
		SchemaVersion: schemaVersion,
		Sequence:      seq,
		Payload:       payload,
	}
	env := c.newEnvelope()
	env.Body = &pb.SessionEnvelope_Data{Data: dm}
	if err := c.sendEnvelope(env); err != nil {
		return fmt.Errorf("pluginapi: send data bytes: %w", err)
	}
	return nil
}

// SendError sends a plugin-originated ErrorMessage to Core.
func (c *Client) SendError(ctx context.Context, code, category, summary, scope, correlationID string) error {
	env := c.newEnvelope()
	env.CorrelationId = correlationID
	env.Body = &pb.SessionEnvelope_Error{
		Error: &pb.ErrorMessage{
			ErrorCode:     code,
			ErrorCategory: category,
			Summary:       summary,
			Scope:         scope,
			CorrelationId: correlationID,
		},
	}
	if err := c.sendEnvelope(env); err != nil {
		return fmt.Errorf("pluginapi: send error: %w", err)
	}
	return nil
}

// Close gracefully closes the session and the connection.
func (c *Client) Close() {
	if c.stream != nil {
		env := c.newEnvelope()
		env.Body = &pb.SessionEnvelope_Session{
			Session: &pb.SessionMessage{Type: pb.SessionMessageType_SESSION_MESSAGE_TYPE_CLOSE},
		}
		_ = c.sendEnvelope(env)
		_ = c.stream.CloseSend()
	}
	_ = c.conn.Close()
}

// sendEnvelope serializes and sends env on the session stream, serializing
// concurrent senders via sendMu (gRPC ClientStream.Send is not goroutine-safe).
func (c *Client) sendEnvelope(env *pb.SessionEnvelope) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(env)
}

func (c *Client) newEnvelope() *pb.SessionEnvelope {
	return &pb.SessionEnvelope{
		MessageId:       newInstanceID(),
		PluginId:        c.cfg.PluginID,
		InstanceId:      c.cfg.InstanceID,
		SessionId:       c.SessionID,
		TimestampUnixMs: time.Now().UnixMilli(),
	}
}

func newInstanceID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
