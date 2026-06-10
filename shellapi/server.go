package shellapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	shellpb "github.com/yoke-project/yoke-proto/gen/go/yoke/shell/v1"
	"google.golang.org/grpc"
)

// CommandHandler is implemented by Core to dispatch shell commands.
type CommandHandler interface {
	// Handle executes cmdText within the authenticated session identified by sessionID.
	// It returns a typed ShellResult. A non-nil error causes ShellCommandComplete{success:false}.
	Handle(ctx context.Context, sessionID, cmdText string) (ShellResult, error)
}

// Event is an asynchronous lifecycle notification delivered to active shell sessions.
type Event struct {
	Type            string
	PluginID        string
	TimestampUnixMS int64
	PayloadJSON     string
}

// Server wraps the gRPC ShellService for use by Core.
type Server struct {
	handler CommandHandler
	grpc    *grpc.Server

	mu       sync.Mutex
	sessions map[string]chan Event // shellSessionID → buffered event channel

	watchMu sync.Mutex
	watches map[string]map[string]struct{} // shellSessionID → set of "pluginID/streamID"
}

// NewServer creates a new ShellService server backed by handler.
func NewServer(handler CommandHandler) *Server {
	s := &Server{
		handler:  handler,
		sessions: make(map[string]chan Event),
		watches:  make(map[string]map[string]struct{}),
	}
	s.grpc = grpc.NewServer()
	shellpb.RegisterShellServiceServer(s.grpc, &shellServiceImpl{server: s})
	return s
}

// WatchStream registers shellSessionID as a subscriber for data events from pluginID/streamID.
func (s *Server) WatchStream(sessionID, pluginID, streamID string) {
	key := pluginID + "/" + streamID
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.watches[sessionID] == nil {
		s.watches[sessionID] = make(map[string]struct{})
	}
	s.watches[sessionID][key] = struct{}{}
}

// UnwatchStream removes the shellSessionID subscription for pluginID/streamID.
func (s *Server) UnwatchStream(sessionID, pluginID, streamID string) {
	key := pluginID + "/" + streamID
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if set := s.watches[sessionID]; set != nil {
		delete(set, key)
	}
}

// OnStreamData delivers a stream data notification to all shell sessions watching
// pluginID/streamID. Only metadata is forwarded — payload bytes are not passed (P1).
func (s *Server) OnStreamData(pluginID, streamID, schemaFamily, schemaVersion string, seq uint64, tsMs int64, payloadBytes int) {
	key := pluginID + "/" + streamID

	s.watchMu.Lock()
	var targets []string
	for sessionID, watched := range s.watches {
		if _, ok := watched[key]; ok {
			targets = append(targets, sessionID)
		}
	}
	s.watchMu.Unlock()

	if len(targets) == 0 {
		return
	}

	ev := Event{
		Type:            "stream.data",
		PluginID:        pluginID,
		TimestampUnixMS: tsMs,
		PayloadJSON: fmt.Sprintf(`{"stream_id":%q,"schema":%q,"seq":%d,"bytes":%d}`,
			streamID, schemaFamily+"/"+schemaVersion, seq, payloadBytes),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sessionID := range targets {
		if ch, ok := s.sessions[sessionID]; ok {
			select {
			case ch <- ev:
			default:
			}
		}
	}
}

// ListenAndServe binds a Unix Domain Socket at socketPath and serves until ctx is done.
func (s *Server) ListenAndServe(ctx context.Context, socketPath string) error {
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("shellapi: listen %s: %w", socketPath, err)
	}
	go func() {
		<-ctx.Done()
		s.grpc.GracefulStop()
	}()
	return s.grpc.Serve(lis)
}

// PushEvent broadcasts ev to all active shell sessions. Delivery is best-effort:
// sessions whose event buffer is full silently drop the event per spec.
func (s *Server) PushEvent(ev Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.sessions {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (s *Server) subscribe(sessionID string) chan Event {
	ch := make(chan Event, 32)
	s.mu.Lock()
	s.sessions[sessionID] = ch
	s.mu.Unlock()
	return ch
}

func (s *Server) unsubscribe(sessionID string) {
	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.mu.Unlock()

	s.watchMu.Lock()
	delete(s.watches, sessionID)
	s.watchMu.Unlock()
}

// --- internal gRPC implementation ---

type shellServiceImpl struct {
	shellpb.UnimplementedShellServiceServer
	server *Server
}

func (s *shellServiceImpl) OpenShell(stream shellpb.ShellService_OpenShellServer) error {
	// First message must be ShellAuthRequest.
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	authReq := msg.GetAuth()
	if authReq == nil {
		_ = stream.Send(&shellpb.OpenShellResponse{
			Payload: &shellpb.OpenShellResponse_Error{
				Error: &shellpb.ShellError{
					ErrorCode:    "AUTH_REQUIRED",
					ErrorMessage: "first message must be ShellAuthRequest",
				},
			},
		})
		return nil
	}

	// Stub auth: accept any non-empty username.
	// Real PAM authentication is performed by Core before delegating to the SDK.
	if authReq.Username == "" {
		_ = stream.Send(&shellpb.OpenShellResponse{
			Payload: &shellpb.OpenShellResponse_AuthResponse{
				AuthResponse: &shellpb.ShellAuthResponse{
					Success:   false,
					ErrorCode: "AUTH_FAILED",
				},
			},
		})
		return nil
	}

	sessionID := newSessionID()
	if err := stream.Send(&shellpb.OpenShellResponse{
		Payload: &shellpb.OpenShellResponse_AuthResponse{
			AuthResponse: &shellpb.ShellAuthResponse{
				Success:        true,
				ShellSessionId: sessionID,
			},
		},
	}); err != nil {
		return err
	}

	// Subscribe to event push for this session. The Core calls PushEvent to
	// inject events; they are forwarded to the stream by the select loop below.
	eventCh := s.server.subscribe(sessionID)
	defer s.server.unsubscribe(sessionID)

	// writeCh serialises all stream.Send calls to a single goroutine, satisfying
	// gRPC's requirement that Send is not called concurrently.
	writeCh := make(chan *shellpb.OpenShellResponse, 64)
	sendErrs := make(chan error, 1)
	go func() {
		for m := range writeCh {
			if err := stream.Send(m); err != nil {
				select {
				case sendErrs <- err:
				default:
				}
				return
			}
		}
	}()
	defer close(writeCh)

	// recvCh delivers client messages from a background goroutine so the main
	// select can multiplex between incoming messages and event push.
	type recvResult struct {
		msg *shellpb.OpenShellRequest
		err error
	}
	recvCh := make(chan recvResult, 4)
	ctx := stream.Context()
	go func() {
		for {
			m, e := stream.Recv()
			select {
			case recvCh <- recvResult{m, e}:
			case <-ctx.Done():
				return
			}
			if e != nil {
				return
			}
		}
	}()

	handler := s.server.handler
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-sendErrs:
			return err

		case ev := <-eventCh:
			writeCh <- &shellpb.OpenShellResponse{
				Payload: &shellpb.OpenShellResponse_Event{
					Event: &shellpb.ShellEventPush{
						EventType:       ev.Type,
						PluginId:        ev.PluginID,
						TimestampUnixMs: ev.TimestampUnixMS,
						PayloadJson:     ev.PayloadJSON,
					},
				},
			}

		case r := <-recvCh:
			if r.err != nil {
				if r.err == io.EOF {
					return nil
				}
				return r.err
			}

			switch p := r.msg.Payload.(type) {
			case *shellpb.OpenShellRequest_Ping:
				writeCh <- &shellpb.OpenShellResponse{
					Payload: &shellpb.OpenShellResponse_Pong{
						Pong: &shellpb.ShellPongResponse{PingId: p.Ping.PingId},
					},
				}

			case *shellpb.OpenShellRequest_Command:
				cmdReq := p.Command
				cmdText := strings.TrimSpace(cmdReq.CommandText)

				if cmdText == "exit" || cmdText == "quit" {
					writeCh <- &shellpb.OpenShellResponse{
						Payload: &shellpb.OpenShellResponse_Complete{
							Complete: &shellpb.ShellCommandComplete{
								CommandId: cmdReq.CommandId,
								Success:   true,
							},
						},
					}
					return nil
				}

				result, handleErr := handler.Handle(ctx, sessionID, cmdText)
				if handleErr == nil {
					line, _ := json.Marshal(result)
					writeCh <- &shellpb.OpenShellResponse{
						Payload: &shellpb.OpenShellResponse_Output{
							Output: &shellpb.ShellCommandOutput{
								CommandId: cmdReq.CommandId,
								Line:      string(line),
							},
						},
					}
				}
				complete := &shellpb.ShellCommandComplete{
					CommandId: cmdReq.CommandId,
					Success:   handleErr == nil,
				}
				if handleErr != nil {
					complete.ErrorCode = handleErr.Error()
				}
				writeCh <- &shellpb.OpenShellResponse{
					Payload: &shellpb.OpenShellResponse_Complete{Complete: complete},
				}

			default:
				writeCh <- &shellpb.OpenShellResponse{
					Payload: &shellpb.OpenShellResponse_Error{
						Error: &shellpb.ShellError{
							ErrorCode:    "UNKNOWN_MESSAGE",
							ErrorMessage: "unrecognised message type",
						},
					},
				}
			}
		}
	}
}

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
