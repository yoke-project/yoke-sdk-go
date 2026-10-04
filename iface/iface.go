// Package iface is the library an interface client is written with: the interface contract, on its
// typed projection, over the base.
package iface

import (
	"context"
	"errors"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"
)

var errNotYet = errors.New("not yet")

// ErrEnded is what a call answers once the attachment has ended.
var ErrEnded = errors.New("the attachment ended")

func Service(runtimeDir, channel string) (string, error) { return "", errNotYet }
func Application(name, channel string, getenv func(string) string) (string, error) {
	return "", errNotYet
}
func Managed(getenv func(string) string) (string, error) { return "", errNotYet }
func Name(r *interfacev1.Request) string                 { return "" }
func SuspensionOf(err error) (grade, by string, ok bool) { return "", "", false }

type Attachment struct {
	Picture  *interfacev1.Snapshot
	Standing *Subscription
	Version  int
}

func Attach(ctx context.Context, address string) (*Attachment, error) { return nil, errNotYet }
func (a *Attachment) Close() error                                    { return nil }
func (a *Attachment) Authenticate(ctx context.Context, account, secret string) (*interfacev1.Authenticated, error) {
	return nil, errNotYet
}
func (a *Attachment) Read(ctx context.Context, kind, identity string) ([]*interfacev1.Record, error) {
	return nil, errNotYet
}
func (a *Attachment) Subscribe(ctx context.Context, f *interfacev1.Filter) (*Subscription, error) {
	return nil, errNotYet
}
func (a *Attachment) Confirm(ctx context.Context, subscription string, sequence uint64) error {
	return errNotYet
}
func (a *Attachment) Command(ctx context.Context, unit, typ string, payload []byte) (*interfacev1.Acknowledged, error) {
	return nil, errNotYet
}
func (a *Attachment) Ask(ctx context.Context, unit, typ string, payload []byte) ([]byte, error) {
	return nil, errNotYet
}
func (a *Attachment) StartStream(ctx context.Context, unit, stream string) (*interfacev1.Acknowledged, error) {
	return nil, errNotYet
}
func (a *Attachment) StopStream(ctx context.Context, unit, stream string) (*interfacev1.Acknowledged, error) {
	return nil, errNotYet
}
func (a *Attachment) SubscribeStream(ctx context.Context, unit, stream string) (*Delivery, error) {
	return nil, errNotYet
}
func (a *Attachment) Reclaim(ctx context.Context) (*interfacev1.Reclaimed, error) {
	return nil, errNotYet
}

type Subscription struct{ ID string }

func (s *Subscription) Next() (*interfacev1.Subscribed, error) { return nil, errNotYet }
func (s *Subscription) Close()                                 {}

type Frame struct {
	Sequence, SentAt uint64
	Payload          []byte
}

type Delivery struct {
	ID      string
	Flowing bool
}

func (d *Delivery) Next() (Frame, error)              { return Frame{}, errNotYet }
func (d *Delivery) Release(ctx context.Context) error { return errNotYet }
