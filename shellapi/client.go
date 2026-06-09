package shellapi

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	shellpb "github.com/yoke-project/yoke-proto/gen/go/yoke/shell/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SessionError is returned by Command and Ping when Core sends a session-level
// ShellError. The stream remains open; callers should display the error and continue.
type SessionError struct {
	Code    string
	Message string
}

func (e *SessionError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type onEventFn = func(eventType, pluginID string, timestampMs int64, payload string)

type cmdResult struct {
	success bool
	errCode string
	sessErr *SessionError
}

// Client is the CLI-side shell client.
type Client struct {
	conn      *grpc.ClientConn
	stream    shellpb.ShellService_OpenShellClient
	SessionID string

	sendMu    sync.Mutex
	outCh     chan string
	doneCh    chan cmdResult
	pongCh    chan string
	closeCh   chan struct{}
	closeOnce sync.Once
	closeErr  error

	// onEvent is written via SetOnEvent and read from the background readLoop goroutine.
	// atomic.Pointer ensures these concurrent accesses are safe.
	onEvent atomic.Pointer[onEventFn]
}

// SetOnEvent registers a callback for server-pushed events.
// Safe to call at any time from any goroutine, including before Auth.
func (c *Client) SetOnEvent(fn onEventFn) {
	if fn == nil {
		c.onEvent.Store(nil)
		return
	}
	c.onEvent.Store(&fn)
}

// Dial opens a connection to shell.sock and initialises the bidirectional stream.
func Dial(ctx context.Context, socketPath string) (*Client, error) {
	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("shellapi: dial %s: %w", socketPath, err)
	}
	svc := shellpb.NewShellServiceClient(conn)
	stream, err := svc.OpenShell(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("shellapi: open stream: %w", err)
	}
	return &Client{
		conn:    conn,
		stream:  stream,
		outCh:   make(chan string, 64),
		doneCh:  make(chan cmdResult, 4),
		pongCh:  make(chan string, 4),
		closeCh: make(chan struct{}),
	}, nil
}

// Auth sends credentials and waits for the auth response.
// On success it starts the background message-reading goroutine.
func (c *Client) Auth(username, password string) error {
	if err := c.stream.Send(&shellpb.ShellClientMessage{
		Payload: &shellpb.ShellClientMessage_Auth{
			Auth: &shellpb.ShellAuthRequest{Username: username, Password: password},
		},
	}); err != nil {
		return fmt.Errorf("shellapi: send auth: %w", err)
	}
	msg, err := c.stream.Recv()
	if err != nil {
		return fmt.Errorf("shellapi: recv auth response: %w", err)
	}
	resp := msg.GetAuthResponse()
	if resp == nil {
		return fmt.Errorf("shellapi: expected ShellAuthResponse, got %T", msg.Payload)
	}
	if !resp.Success {
		return fmt.Errorf("shellapi: authentication failed: %s", resp.ErrorCode)
	}
	c.SessionID = resp.ShellSessionId
	go c.readLoop()
	return nil
}

// readLoop drains the inbound gRPC stream and routes each message to the
// appropriate channel or callback. Runs until the stream closes.
func (c *Client) readLoop() {
	defer c.closeOnce.Do(func() { close(c.closeCh) })
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.closeErr = err
			}
			return
		}
		switch payload := msg.Payload.(type) {
		case *shellpb.ShellServerMessage_Output:
			select {
			case c.outCh <- payload.Output.Line:
			default:
			}
		case *shellpb.ShellServerMessage_Complete:
			c.doneCh <- cmdResult{success: payload.Complete.Success, errCode: payload.Complete.ErrorCode}
		case *shellpb.ShellServerMessage_Pong:
			select {
			case c.pongCh <- payload.Pong.PingId:
			default:
			}
		case *shellpb.ShellServerMessage_Event:
			ev := payload.Event
			if fn := c.onEvent.Load(); fn != nil {
				(*fn)(ev.EventType, ev.PluginId, ev.TimestampUnixMs, ev.PayloadJson)
			}
		case *shellpb.ShellServerMessage_Error:
			c.doneCh <- cmdResult{sessErr: &SessionError{
				Code:    payload.Error.ErrorCode,
				Message: payload.Error.ErrorMessage,
			}}
		}
	}
}

// Command sends a command and returns all output lines (blocks until ShellCommandComplete).
func (c *Client) Command(ctx context.Context, commandID, commandText string) (lines []string, success bool, errCode string, err error) {
	c.sendMu.Lock()
	sendErr := c.stream.Send(&shellpb.ShellClientMessage{
		Payload: &shellpb.ShellClientMessage_Command{
			Command: &shellpb.ShellCommandRequest{
				CommandId:   commandID,
				CommandText: commandText,
			},
		},
	})
	c.sendMu.Unlock()
	if sendErr != nil {
		return nil, false, "", fmt.Errorf("shellapi: send command: %w", sendErr)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, false, "", ctx.Err()
		case <-c.closeCh:
			if c.closeErr != nil {
				return nil, false, "", c.closeErr
			}
			return lines, true, "", io.EOF
		case line := <-c.outCh:
			lines = append(lines, line)
		case result := <-c.doneCh:
			if result.sessErr != nil {
				return nil, false, "", result.sessErr
			}
			return lines, result.success, result.errCode, nil
		}
	}
}

// Ping sends a keepalive ping and waits for the matching pong.
func (c *Client) Ping(ctx context.Context, pingID string) error {
	c.sendMu.Lock()
	err := c.stream.Send(&shellpb.ShellClientMessage{
		Payload: &shellpb.ShellClientMessage_Ping{
			Ping: &shellpb.ShellPingRequest{PingId: pingID},
		},
	})
	c.sendMu.Unlock()
	if err != nil {
		return fmt.Errorf("shellapi: send ping: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeCh:
			if c.closeErr != nil {
				return c.closeErr
			}
			return io.EOF
		case gotID := <-c.pongCh:
			if gotID != pingID {
				return fmt.Errorf("shellapi: pong id mismatch: got %q want %q", gotID, pingID)
			}
			return nil
		}
	}
}

// Close closes the stream and the underlying connection.
func (c *Client) Close() error {
	_ = c.stream.CloseSend()
	return c.conn.Close()
}
