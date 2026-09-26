// Package base is what the three libraries of this project share and nothing else: connecting to a
// socket, the envelope and its correlation, a refusal as a Go error carrying its code, the addresses a
// party computes from its environment, and the contract version each library states.
//
// A concept that exists on one contract only does not live here: a candidate is in the base only if
// all three libraries would otherwise implement it.
package base

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"
)

// PluginContract is the version of the plugin contract the definitions carry, which the plugin library
// declares.
const PluginContract = int(pluginv1.Contract_CONTRACT_VERSION)

// Env is what a party is handed, and all it may assume.
type Env struct {
	Plugin string // the plugin this unit is a copy of
	Unit   string // this unit's identity
	Socket string // the instance's plugin channel
	Bind   string // the path this unit is expected to bind
	Token  string // the bootstrap token
}

// Environment reads the reserved variables. A missing one is an error naming it; no path is assumed.
func Environment(getenv func(string) string) (Env, error) {
	e := Env{Plugin: getenv("YOKE_PLUGIN"), Unit: getenv("YOKE_UNIT"), Socket: getenv("YOKE_SOCKET"), Bind: getenv("YOKE_BIND"), Token: getenv("YOKE_TOKEN")}
	var missing []string
	for name, value := range map[string]string{"YOKE_UNIT": e.Unit, "YOKE_SOCKET": e.Socket, "YOKE_BIND": e.Bind} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return Env{}, fmt.Errorf("the environment does not carry %s", strings.Join(missing, ", "))
	}
	return e, nil
}

// Dial connects to a Unix socket.
func Dial(path string) (*grpc.ClientConn, error) {
	return grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Refusal is a refusal as it travels — a code from the one namespace, a message for a person, and the
// stage where there is one — in Go's error idiom.
type Refusal struct {
	Code    string
	Message string
	Stage   string
}

func (r *Refusal) Error() string {
	if r.Stage != "" {
		return fmt.Sprintf("%s at %s: %s", r.Code, r.Stage, r.Message)
	}
	return r.Code + ": " + r.Message
}

// RefusalOf is the refusal an error envelope carries.
func RefusalOf(e *pluginv1.Error) error {
	return &Refusal{Code: e.GetCode(), Message: e.GetMessage()}
}

// IsRefusal says whether err is a refusal with the code given.
func IsRefusal(err error, code string) bool {
	var r *Refusal
	return errors.As(err, &r) && r.Code == code
}

// Envelopes fills the header of every envelope one party sends in one Session: a message identity no
// other of its envelopes has, the Session's identity, and the sender's clock.
type Envelopes struct {
	session string
	mu      sync.Mutex
	next    int
}

// NewEnvelopes numbers the envelopes of one Session.
func NewEnvelopes(session string) *Envelopes { return &Envelopes{session: session} }

// Seal fills e's header.
func (s *Envelopes) Seal(e *pluginv1.Envelope) *pluginv1.Envelope {
	s.mu.Lock()
	s.next++
	n := s.next
	s.mu.Unlock()
	e.MessageId, e.SessionId, e.SentAtUnixNano = fmt.Sprintf("u-%d", n), s.session, time.Now().UnixNano()
	return e
}

// Answer fills e's header as an answer to the message identified by to.
func (s *Envelopes) Answer(to string, e *pluginv1.Envelope) *pluginv1.Envelope {
	s.Seal(e)
	e.CorrelationId = to
	return e
}
