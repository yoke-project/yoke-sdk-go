package iface_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
)

// std: yoke-sdk-go:the-interface-library.11
func TestAQuestionNamesItsCause(t *testing.T) {
	c := &core{opening: opening(1), answer: func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		if r.GetQuery().GetCause() == "" {
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_Query{Query: &interfacev1.Answered{Payload: []byte("plain")}}}))
			return
		}
		send(&interfacev1.CoreFrame{Call: call, Carries: &interfacev1.CoreFrame_Refusal{Refusal: &interfacev1.Refusal{Code: "use.cause.unknown",
			Message: "no such question", Detail: &interfacev1.Refusal_Item{Item: r.GetQuery().GetCause()}}}})
	}}
	a := attach(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if got, err := a.Ask(ctx, "source", "station.status", nil); err != nil || string(got) != "plain" {
		t.Fatalf("a question with no cause was answered %q, %v", got, err)
	}
	_, err := a.AskCaused(ctx, "q-7", "source", "station.status", []byte("why"))
	var r *base.Refusal
	if !base.IsRefusal(err, "use.cause.unknown") || !errors.As(err, &r) || r.Item != "q-7" {
		t.Fatalf("a question naming a cause was answered %v", err)
	}
	seen := c.seen()
	if len(seen) != 2 || seen[0].GetQuery().GetCause() != "" || seen[1].GetQuery().GetCause() != "q-7" ||
		seen[1].GetQuery().GetUnit() != "source" || string(seen[1].GetQuery().GetPayload()) != "why" {
		t.Fatalf("the Core was sent %v", seen)
	}
}

// std: yoke-sdk-go:the-interface-library.12
func TestAFollowedStreamIsSubscribedAgainWhenItsDeliveryEnds(t *testing.T) {
	var mu sync.Mutex
	subscriptions, absent := 0, 0
	c := &core{opening: opening(1), answer: func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.GetStreamStart() != nil:
			// Between the two lives, the unit is not running: the follow waits it out.
			if subscriptions == 1 && absent < 2 {
				absent++
				send(&interfacev1.CoreFrame{Call: call, Carries: &interfacev1.CoreFrame_Refusal{Refusal: &interfacev1.Refusal{Code: "unit.not_running",
					Detail: &interfacev1.Refusal_Subject{Subject: &interfacev1.Subject{Kind: "unit", Identity: "source"}}}}})
				return
			}
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamStart{StreamStart: &interfacev1.Acknowledged{}}}))
		case r.GetStreamSubscribe() != nil:
			subscriptions++
			id := fmt.Sprintf("d-%d", subscriptions)
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{
				Delivery: id, Flowing: true, Arrives: &interfacev1.Delivering_Connection{Connection: true}}}}))
			go func(n int) {
				time.Sleep(20 * time.Millisecond)
				send(&interfacev1.CoreFrame{Call: id, Carries: &interfacev1.CoreFrame_Delivery{Delivery: &interfacev1.StreamDelivery{
					Delivery: id, Sequence: 1, Payload: []byte(fmt.Sprintf("life %d", n))}}})
				if n == 1 {
					// The unit's Session ended: so did the delivery.
					send(&interfacev1.CoreFrame{Call: id, Carries: &interfacev1.CoreFrame_Completion{Completion: &interfacev1.Completion{By: interfacev1.Completion_BY_CORE}}})
				}
			}(subscriptions)
		default:
			send(answered(call, &interfacev1.Response{}))
		}
	}}
	a := attach(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f, err := a.Follow(ctx, "source", "station.processed")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"life 1", "life 2"} {
		frame, err := f.Next()
		if err != nil || string(frame.Payload) != want || frame.Sequence != 1 || f.Delivery() != fmt.Sprintf("d-%d", i+1) {
			t.Fatalf("frame %d: %q on %s, %v", i+1, frame.Payload, f.Delivery(), err)
		}
	}
	starts := 0
	for _, r := range c.seen() {
		if r.GetStreamStart() != nil {
			starts++
		}
	}
	if starts != 4 || absent != 2 {
		t.Fatalf("the stream was started %d times, refused %d", starts, absent)
	}
	if err := f.Close(ctx); err != nil {
		t.Fatalf("closing the follow: %v", err)
	}
	if _, err := f.Next(); err == nil {
		t.Fatal("a closed follow gave a frame")
	}
}
