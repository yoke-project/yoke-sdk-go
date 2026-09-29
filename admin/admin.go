// Package admin is the library tooling and a person's session are written with: the administrative
// contract, on either of its two projections, over the base.
package admin

import (
	"context"
	"errors"
	"time"

	administrativev1 "github.com/yoke-project/yoke/proto/yoke/administrative/v1"
)

// Contract is the version of the administrative contract this library covers, stated on every request.
const Contract = int(administrativev1.Contract_CONTRACT_VERSION)

var errNotYet = errors.New("not yet")

// Addresses are an instance's two administrative sockets.
type Addresses struct {
	Operator string
	Shell    string
}

// Service is the service form's addresses, under its runtime directory.
func Service(runtimeDir string) Addresses { return Addresses{} }

// Application is an application instance's addresses, derived from its name under the account's runtime
// directory.
func Application(name string, getenv func(string) string) (Addresses, error) {
	return Addresses{}, errNotYet
}

// Effective is when a change takes effect.
type Effective int

const (
	Immediately Effective = iota + 1
	AtNextAdmission
)

// Consequence is what a change did to one unit.
type Consequence struct {
	Unit        string
	Incarnation uint64
	What        string
}

// Changed is what every change answers: what it replaced, when it takes effect, and one consequence per
// unit affected.
type Changed struct {
	Previously   *administrativev1.Previously
	Effective    Effective
	Consequences []Consequence
}

// Retention is a retention override; each limit left out is unconstrained.
type Retention struct {
	Age     time.Duration
	Bytes   *uint64
	Entries *uint64
}

// Operations are the contract's operations, one method each, on whichever projection carries them.
type Operations struct{}

func (o Operations) EnablePlugin(ctx context.Context, plugin string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) DisablePlugin(ctx context.Context, plugin string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) Grant(ctx context.Context, plugin, capability string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) Withdraw(ctx context.Context, plugin, capability string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) StartUnit(ctx context.Context, unit string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) StopUnit(ctx context.Context, unit string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) RestartUnit(ctx context.Context, unit string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) StartStream(ctx context.Context, unit, stream string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) StopStream(ctx context.Context, unit, stream string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) SetRetention(ctx context.Context, unit string, r Retention) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) ClearRetention(ctx context.Context, unit string) (Changed, error) {
	return Changed{}, errNotYet
}
func (o Operations) Ask(ctx context.Context, unit, typ string, question []byte) ([]byte, error) {
	return nil, errNotYet
}
func (o Operations) Read(ctx context.Context, kind, identity string) ([]*administrativev1.Record, error) {
	return nil, errNotYet
}
func (o Operations) QueryLog(ctx context.Context, q *administrativev1.LogQuery) (*administrativev1.LogPage, error) {
	return nil, errNotYet
}
func (o Operations) FollowLog(ctx context.Context, f *administrativev1.LogFollow) (*Follow, error) {
	return nil, errNotYet
}
func (o Operations) Subscribe(ctx context.Context, f *administrativev1.Filter) (*Subscription, error) {
	return nil, errNotYet
}

// Follow is a follow of the log, delivered in order.
type Follow struct{}

// Next is the next entry, or where the follow fell behind; an error once it ends.
func (f *Follow) Next() (*administrativev1.Followed, error) { return nil, errNotYet }

// Subscription is a subscription, delivered in order: its snapshot, then events and overflows.
type Subscription struct{}

// Next is the next delivery; an error once the subscription ends.
func (s *Subscription) Next() (*administrativev1.Subscribed, error) { return nil, errNotYet }

// Operator is the operator projection: a call and its answer, or a call answered by a stream.
type Operator struct{ Operations }

// DialOperator reaches the operator projection at path.
func DialOperator(path string) (*Operator, error) { return nil, errNotYet }

// Close lets the connection go.
func (o *Operator) Close() error { return nil }

// Shell is a connection held open on the shell projection.
type Shell struct {
	Operations
	// Connection is the identity the Core issued, and Person the actor the channel established.
	Connection string
	Person     string
	// Standing is the subscription the connection holds from the moment it opened.
	Standing *Subscription
}

// Connect opens a connection on the shell projection at path.
func Connect(ctx context.Context, path string) (*Shell, error) { return nil, errNotYet }

// Close ends the connection.
func (s *Shell) Close() error { return nil }
