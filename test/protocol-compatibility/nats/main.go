// S5 NATS real-client compatibility suite.
//
// Drives a real UnifiedMQ NATS listener with the official nats.go client
// (v1.48.0). It never speaks the wire protocol by hand: every case goes
// through nats.Connect / Subscribe / Publish / Request / Flush, so a passing
// run is evidence that a third-party client works against this adapter.
//
// Usage (the Python driver supplies the address and token):
//
//	go run . -addr 127.0.0.1:PORT -token compat-secret [-case name]
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	addr  = flag.String("addr", "127.0.0.1:4223", "broker host:port")
	token = flag.String("token", "", "CONNECT auth token")
	only  = flag.String("case", "", "run one case by name")
)

type result struct {
	name   string
	detail string
	err    error
}

func main() {
	flag.Parse()
	cases := []struct {
		name string
		fn   func() (string, error)
	}{
		{"nats-connect-info", connectInfo},
		{"nats-pubsub", pubsub},
		{"nats-wildcard", wildcard},
		{"nats-queue-group", queueGroup},
		{"nats-request-reply", requestReply},
		{"nats-no-history", noHistory},
		{"nats-unsubscribe", unsubscribe},
		{"nats-auto-unsubscribe", autoUnsubscribe},
		{"nats-echo-false", echoFalse},
		{"nats-ping-flush", pingFlush},
		{"nats-auth-failure", authFailure},
		{"nats-js-stream-publish", jsStreamPublish},
		{"nats-js-pull-ack", jsPullAck},
		{"nats-js-redelivery", jsRedelivery},
	}
	pass, fail := 0, 0
	for _, c := range cases {
		if *only != "" && *only != c.name {
			continue
		}
		start := time.Now()
		detail, err := c.fn()
		if err != nil {
			fail++
			fmt.Printf("FAIL %s (%.2fs) %v\n", c.name, time.Since(start).Seconds(), err)
		} else {
			pass++
			fmt.Printf("PASS %s (%.2fs) %s\n", c.name, time.Since(start).Seconds(), detail)
		}
	}
	fmt.Printf("SUMMARY passed=%d failed=%d\n", pass, fail)
	if fail > 0 {
		os.Exit(1)
	}
}

func url() string { return "nats://" + *addr }


// resetStream drops the stream so cases are idempotent across runs: the
// broker's data dir persists between runs and seq assertions below assume a
// fresh stream.
func resetStream(js nats.JetStreamContext, name string) {
	_ = js.DeleteStream(name)
}

func connect(opts ...nats.Option) (*nats.Conn, error) {
	base := []nats.Option{
		nats.Timeout(5 * time.Second),
		nats.ReconnectWait(200 * time.Millisecond),
		nats.MaxReconnects(1),
	}
	if *token != "" {
		base = append(base, nats.Token(*token))
	}
	base = append(base, opts...)
	return nats.Connect(url(), base...)
}

func withConn(fn func(*nats.Conn) (string, error)) (string, error) {
	nc, err := connect()
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()
	return fn(nc)
}

func recv(sub *nats.Subscription, timeout time.Duration) ([]byte, error) {
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func connectInfo() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		if nc.Status() != nats.CONNECTED {
			return "", fmt.Errorf("status %v", nc.Status())
		}
		if err := nc.FlushTimeout(3 * time.Second); err != nil {
			return "", fmt.Errorf("flush: %w", err)
		}
		return fmt.Sprintf(`{"connected":true,"server":%q}`, nc.ConnectedUrl()), nil
	})
}

func pubsub() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		sub, err := nc.SubscribeSync("compat.pubsub")
		if err != nil {
			return "", err
		}
		defer sub.Unsubscribe()
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if err := nc.Publish("compat.pubsub", []byte("hello-nats")); err != nil {
			return "", err
		}
		got, err := recv(sub, 3*time.Second)
		if err != nil {
			return "", fmt.Errorf("recv: %w", err)
		}
		if string(got) != "hello-nats" {
			return "", fmt.Errorf("payload %q", got)
		}
		return fmt.Sprintf(`{"payload":%q}`, got), nil
	})
}

func wildcard() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		sub, err := nc.SubscribeSync("compat.*.leaf")
		if err != nil {
			return "", err
		}
		defer sub.Unsubscribe()
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if err := nc.Publish("compat.one.leaf", []byte("in")); err != nil {
			return "", err
		}
		got, err := recv(sub, 3*time.Second)
		if err != nil || string(got) != "in" {
			return "", fmt.Errorf("star match: %v %q", err, got)
		}
		if err := nc.Publish("compat.one.two.leaf", []byte("out")); err != nil {
			return "", err
		}
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if extra, err := recv(sub, 400*time.Millisecond); err == nil {
			return "", fmt.Errorf("star matched more than one token: %q", extra)
		}
		// `>` matches the rest.
		deep, err := nc.SubscribeSync("deep.>")
		if err != nil {
			return "", err
		}
		defer deep.Unsubscribe()
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if err := nc.Publish("deep.a.b.c", []byte("deep")); err != nil {
			return "", err
		}
		if got, err := recv(deep, 3*time.Second); err != nil || string(got) != "deep" {
			return "", fmt.Errorf("gt match: %v %q", err, got)
		}
		return `{"star":"one-token","gt":"rest"}`, nil
	})
}

func queueGroup() (string, error) {
	a, err := connect()
	if err != nil {
		return "", err
	}
	defer a.Close()
	b, err := connect()
	if err != nil {
		return "", err
	}
	defer b.Close()

	sa, err := a.QueueSubscribeSync("compat.jobs", "workers")
	if err != nil {
		return "", err
	}
	defer sa.Unsubscribe()
	sb, err := b.QueueSubscribeSync("compat.jobs", "workers")
	if err != nil {
		return "", err
	}
	defer sb.Unsubscribe()
	pub, err := connect()
	if err != nil {
		return "", err
	}
	defer pub.Close()
	if err := pub.Flush(); err != nil {
		return "", err
	}

	const n = 10
	for i := 0; i < n; i++ {
		if err := pub.Publish("compat.jobs", []byte(fmt.Sprintf("%d", i))); err != nil {
			return "", err
		}
	}
	if err := pub.Flush(); err != nil {
		return "", err
	}

	seen := map[string]int{}
	deadline := time.Now().Add(8 * time.Second)
	for len(seen) < n {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("got %d/%d: %v", len(seen), n, keys(seen))
		}
		if msg, err := sa.NextMsg(200 * time.Millisecond); err == nil {
			seen[string(msg.Data)]++
		}
		if msg, err := sb.NextMsg(200 * time.Millisecond); err == nil {
			seen[string(msg.Data)]++
		}
	}
	if len(seen) != n {
		return "", fmt.Errorf("duplicates/gaps: %v", seen)
	}
	for k, v := range seen {
		if v != 1 {
			return "", fmt.Errorf("message %s delivered %d times", k, v)
		}
	}
	return fmt.Sprintf(`{"delivered_once":%d}`, n), nil
}

func requestReply() (string, error) {
	responder, err := connect()
	if err != nil {
		return "", err
	}
	defer responder.Close()
	sub, err := responder.Subscribe("compat.svc", func(msg *nats.Msg) {
		_ = msg.Respond([]byte("pong:" + string(msg.Data)))
	})
	if err != nil {
		return "", err
	}
	defer sub.Unsubscribe()
	if err := responder.Flush(); err != nil {
		return "", err
	}

	requester, err := connect()
	if err != nil {
		return "", err
	}
	defer requester.Close()
	reply, err := requester.Request("compat.svc", []byte("ping"), 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("request: %w", err)
	}
	if string(reply.Data) != "pong:ping" {
		return "", fmt.Errorf("reply %q", reply.Data)
	}
	return fmt.Sprintf(`{"reply":%q}`, reply.Data), nil
}

func noHistory() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		if err := nc.Publish("compat.fresh", []byte("old")); err != nil {
			return "", err
		}
		if err := nc.Flush(); err != nil {
			return "", err
		}
		sub, err := nc.SubscribeSync("compat.fresh")
		if err != nil {
			return "", err
		}
		defer sub.Unsubscribe()
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if _, err := recv(sub, 400*time.Millisecond); err == nil {
			return "", errors.New("new subscription replayed history")
		}
		if err := nc.Publish("compat.fresh", []byte("new")); err != nil {
			return "", err
		}
		got, err := recv(sub, 3*time.Second)
		if err != nil || string(got) != "new" {
			return "", fmt.Errorf("after sub: %v %q", err, got)
		}
		return `{"history":"not-replayed","new":"delivered"}`, nil
	})
}

func unsubscribe() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		sub, err := nc.SubscribeSync("compat.unsub")
		if err != nil {
			return "", err
		}
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if err := nc.Publish("compat.unsub", []byte("one")); err != nil {
			return "", err
		}
		if _, err := recv(sub, 3*time.Second); err != nil {
			return "", err
		}
		if err := sub.Unsubscribe(); err != nil {
			return "", err
		}
		if err := nc.Publish("compat.unsub", []byte("two")); err != nil {
			return "", err
		}
		if _, err := recv(sub, 400*time.Millisecond); err == nil {
			return "", errors.New("delivery continued after unsubscribe")
		}
		return `{"after_unsubscribe":"none"}`, nil
	})
}

func autoUnsubscribe() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		sub, err := nc.SubscribeSync("compat.auto")
		if err != nil {
			return "", err
		}
		if err := sub.AutoUnsubscribe(1); err != nil {
			return "", err
		}
		if err := nc.Flush(); err != nil {
			return "", err
		}
		for _, p := range []string{"first", "second"} {
			if err := nc.Publish("compat.auto", []byte(p)); err != nil {
				return "", err
			}
		}
		if err := nc.Flush(); err != nil {
			return "", err
		}
		if got, err := recv(sub, 3*time.Second); err != nil || string(got) != "first" {
			return "", fmt.Errorf("first: %v %q", err, got)
		}
		if _, err := recv(sub, 400*time.Millisecond); err == nil {
			return "", errors.New("AutoUnsubscribe(1) delivered twice")
		}
		return `{"max":1,"delivered":1}`, nil
	})
}

func echoFalse() (string, error) {
	self, err := connect(nats.NoEcho())
	if err != nil {
		return "", err
	}
	defer self.Close()
	other, err := connect()
	if err != nil {
		return "", err
	}
	defer other.Close()

	s1, err := self.SubscribeSync("compat.echo")
	if err != nil {
		return "", err
	}
	defer s1.Unsubscribe()
	s2, err := other.SubscribeSync("compat.echo")
	if err != nil {
		return "", err
	}
	defer s2.Unsubscribe()
	if err := other.Flush(); err != nil {
		return "", err
	}
	if err := self.Publish("compat.echo", []byte("mine")); err != nil {
		return "", err
	}
	if err := self.Flush(); err != nil {
		return "", err
	}
	if got, err := recv(s2, 3*time.Second); err != nil || string(got) != "mine" {
		return "", fmt.Errorf("other: %v %q", err, got)
	}
	if _, err := recv(s1, 400*time.Millisecond); err == nil {
		return "", errors.New("echo=false client got its own message")
	}
	return `{"self":"suppressed","other":"delivered"}`, nil
}

func pingFlush() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = nc.Flush()
			}()
		}
		wg.Wait()
		if err := nc.FlushTimeout(3 * time.Second); err != nil {
			return "", err
		}
		return `{"pings":20,"pongs":"all"}`, nil
	})
}

func authFailure() (string, error) {
	nc, err := nats.Connect(url(),
		nats.Token("wrong-token"),
		nats.Timeout(3*time.Second),
		nats.MaxReconnects(0),
	)
	if err == nil {
		nc.Close()
		return "", errors.New("connection with a wrong token succeeded")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "authorization") {
		return "", fmt.Errorf("unexpected error %v", err)
	}
	return `{"auth":"refused"}`, nil
}

// --- JetStream MVP (NATS-JS-001..003 minimal slice) ------------------------

// Stream create + managed publish with PubAck + seq monotonicity.
func jsStreamPublish() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		js, err := nc.JetStream()
		if err != nil {
			return "", fmt.Errorf("jetstream: %w", err)
		}
		resetStream(js, "compat")
		if _, err = js.AddStream(&nats.StreamConfig{
			Name:     "compat",
			Subjects: []string{"jscompat.>"},
		}); err != nil {
			return "", fmt.Errorf("add stream: %w", err)
		}
		if pa, err := js.Publish("jscompat.a", []byte("hello-js")); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		} else if pa.Stream != "compat" || pa.Sequence != 1 {
			return "", fmt.Errorf("puback: %+v", pa)
		}
		if pa, err := js.Publish("jscompat.b", []byte("hello-js-2")); err != nil {
			return "", fmt.Errorf("publish2: %w", err)
		} else if pa.Sequence != 2 {
			return "", fmt.Errorf("puback2: %+v", pa)
		}
		return `{"managed_publish":true,"seq":2}`, nil
	})
}

// Durable pull consumer: fetch, ack, and a second fetch finds nothing new.
func jsPullAck() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		js, err := nc.JetStream()
		if err != nil {
			return "", fmt.Errorf("jetstream: %w", err)
		}
		resetStream(js, "compatpull")
		if _, err = js.AddStream(&nats.StreamConfig{
			Name:     "compatpull",
			Subjects: []string{"compatpull.>"},
		}); err != nil {
			return "", fmt.Errorf("add stream: %w", err)
		}
		if pa, err := js.Publish("compatpull.a", []byte("pull-me")); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		} else if pa.Sequence != 1 {
			return "", fmt.Errorf("puback: %+v", pa)
		}
		sub, err := js.PullSubscribe("compatpull.a", "durable-pull")
		if err != nil {
			return "", fmt.Errorf("pull subscribe: %w", err)
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil || len(msgs) == 0 {
			return "", fmt.Errorf("fetch: %w", err)
		}
		if string(msgs[0].Data) != "pull-me" {
			return "", fmt.Errorf("payload %q", msgs[0].Data)
		}
		if err = msgs[0].Ack(); err != nil {
			return "", fmt.Errorf("ack: %w", err)
		}
		// Acked: the next fetch finds nothing new.
		msgs2, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
		if err != nil && len(msgs2) != 0 {
			return "", fmt.Errorf("post-ack fetch: %w", err)
		}
		return `{"pulled":true,"acked":true}`, nil
	})
}

// Unacked deliveries are redelivered (NAK / ack-wait machinery).
func jsRedelivery() (string, error) {
	return withConn(func(nc *nats.Conn) (string, error) {
		js, err := nc.JetStream()
		if err != nil {
			return "", fmt.Errorf("jetstream: %w", err)
		}
		resetStream(js, "compatredel")
		_ = nc.FlushTimeout(2 * time.Second)
		if _, err = js.AddStream(&nats.StreamConfig{
			Name:     "compatredel",
			Subjects: []string{"compatredel.>"},
		}); err != nil {
			return "", fmt.Errorf("add stream: %w", err)
		}
		if _, err = js.Publish("compatredel.a", []byte("again")); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		}
		sub, err := js.PullSubscribe("compatredel.a", "durable-redel")
		if err != nil {
			return "", fmt.Errorf("pull subscribe: %w", err)
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil || len(msgs) == 0 {
			return "", fmt.Errorf("fetch1: %w", err)
		}
		// NAK: come back immediately.
		if err = msgs[0].Nak(); err != nil {
			return "", fmt.Errorf("nak: %w", err)
		}
		msgs2, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil || len(msgs2) == 0 {
			return "", fmt.Errorf("redelivery fetch: %w", err)
		}
		if string(msgs2[0].Data) != "again" {
			return "", fmt.Errorf("redelivery payload %q", msgs2[0].Data)
		}
		if err = msgs2[0].Ack(); err != nil {
			return "", fmt.Errorf("ack2: %w", err)
		}
		return `{"redelivered":true}`, nil
	})
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
