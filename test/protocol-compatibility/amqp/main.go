// S5 AMQP real-client compatibility suite.
//
// Drives a real UnifiedMQ AMQP 0-9-1 listener with the official
// github.com/rabbitmq/amqp091-go client (v1.10.0). It never speaks the wire
// protocol by hand: every case goes through amqp.Dial / Channel /
// ExchangeDeclare / QueueDeclare / QueueBind / Consume / Publish / Ack /
// Nack / Qos, so a passing run is evidence that a third-party client works
// against this adapter.
//
// Usage (the Python driver supplies the address):
//
//	./amqp_compat -addr 127.0.0.1:PORT [-case name]
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	addr = flag.String("addr", "127.0.0.1:5673", "broker host:port")
	only = flag.String("case", "", "run one case by name")
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
		{"amqp-connect", connect},
		{"amqp-pubsub", pubsub},
		{"amqp-direct", directRouting},
		{"amqp-fanout", fanoutRouting},
		{"amqp-topic", topicRouting},
		{"amqp-competing-consumers", competingConsumers},
		{"amqp-manual-ack", manualAck},
		{"amqp-nack-requeue", nackRequeue},
		{"amqp-unbind", unbind},
		{"amqp-queue-delete", queueDelete},
		{"amqp-passive-missing", passiveMissing},
		{"amqp-prefetch-qos", prefetchQos},
		{"amqp-multi-channel", multiChannel},
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

func dial() (*amqp.Connection, error) {
	return amqp.DialConfig("amqp://guest:guest@"+*addr+"/",
		amqp.Config{Heartbeat: 0, Locale: "en_US"})
}

func withConn(fn func(*amqp.Connection, *amqp.Channel) (string, error)) (string, error) {
	conn, err := dial()
	if err != nil {
		return "", fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return "", fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()
	return fn(conn, ch)
}

func declareQueue(ch *amqp.Channel, name string) error {
	_, err := ch.QueueDeclare(name, false, false, false, false, nil)
	return err
}

func publish(ch *amqp.Channel, exchange, key, body string) error {
	return ch.Publish(exchange, key, false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte(body)})
}

func recvOne(msgs <-chan amqp.Delivery, timeout time.Duration) (amqp.Delivery, error) {
	select {
	case m, ok := <-msgs:
		if !ok {
			return amqp.Delivery{}, errors.New("delivery channel closed")
		}
		return m, nil
	case <-time.After(timeout):
		return amqp.Delivery{}, errors.New("timeout waiting for delivery")
	}
}

func expectNoDelivery(msgs <-chan amqp.Delivery, timeout time.Duration, why string) error {
	select {
	case m, ok := <-msgs:
		if !ok {
			return nil
		}
		return fmt.Errorf("%s: unexpected delivery %q", why, m.Body)
	case <-time.After(timeout):
		return nil
	}
}

func connect() (string, error) {
	return withConn(func(conn *amqp.Connection, ch *amqp.Channel) (string, error) {
		if conn.IsClosed() {
			return "", errors.New("connection closed right after dial")
		}
		return fmt.Sprintf(`{"connected":true,"channel":%v}`, !ch.IsClosed()), nil
	})
}

func pubsub() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := ch.ExchangeDeclare("compat.direct", "direct", false, false, false, false, nil); err != nil {
			return "", fmt.Errorf("exchange: %w", err)
		}
		if err := declareQueue(ch, "compat.q"); err != nil {
			return "", fmt.Errorf("queue: %w", err)
		}
		if err := ch.QueueBind("compat.q", "rk", "compat.direct", false, nil); err != nil {
			return "", fmt.Errorf("bind: %w", err)
		}
		msgs, err := ch.Consume("compat.q", "", false, false, false, false, nil)
		if err != nil {
			return "", fmt.Errorf("consume: %w", err)
		}
		if err := publish(ch, "compat.direct", "rk", "hello-amqp"); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		}
		m, err := recvOne(msgs, 5*time.Second)
		if err != nil {
			return "", err
		}
		if string(m.Body) != "hello-amqp" {
			return "", fmt.Errorf("body %q", m.Body)
		}
		if err := m.Ack(false); err != nil {
			return "", fmt.Errorf("ack: %w", err)
		}
		return fmt.Sprintf(`{"body":%q}`, m.Body), nil
	})
}

func directRouting() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := ch.ExchangeDeclare("compat.rr", "direct", false, false, false, false, nil); err != nil {
			return "", err
		}
		if err := declareQueue(ch, "compat.rr.hit"); err != nil {
			return "", err
		}
		if err := declareQueue(ch, "compat.rr.miss"); err != nil {
			return "", err
		}
		if err := ch.QueueBind("compat.rr.hit", "yes", "compat.rr", false, nil); err != nil {
			return "", err
		}
		if err := ch.QueueBind("compat.rr.miss", "no", "compat.rr", false, nil); err != nil {
			return "", err
		}
		hit, err := ch.Consume("compat.rr.hit", "", true, false, false, false, nil)
		if err != nil {
			return "", err
		}
		miss, err := ch.Consume("compat.rr.miss", "", true, false, false, false, nil)
		if err != nil {
			return "", err
		}
		if err := publish(ch, "compat.rr", "yes", "direct-hit"); err != nil {
			return "", err
		}
		m, err := recvOne(hit, 5*time.Second)
		if err != nil || string(m.Body) != "direct-hit" {
			return "", fmt.Errorf("hit: %v %q", err, m.Body)
		}
		if err := expectNoDelivery(miss, 600*time.Millisecond, "routing key mismatch"); err != nil {
			return "", err
		}
		return `{"matched":"yes","unmatched":"no"}`, nil
	})
}

func fanoutRouting() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := ch.ExchangeDeclare("compat.fanout", "fanout", false, false, false, false, nil); err != nil {
			return "", err
		}
		var chans []<-chan amqp.Delivery
		for _, q := range []string{"compat.fanout.a", "compat.fanout.b", "compat.fanout.c"} {
			if err := declareQueue(ch, q); err != nil {
				return "", err
			}
			if err := ch.QueueBind(q, "", "compat.fanout", false, nil); err != nil {
				return "", err
			}
			msgs, err := ch.Consume(q, "", true, false, false, false, nil)
			if err != nil {
				return "", err
			}
			chans = append(chans, msgs)
		}
		if err := publish(ch, "compat.fanout", "ignored-key", "fanout-body"); err != nil {
			return "", err
		}
		for i, msgs := range chans {
			m, err := recvOne(msgs, 5*time.Second)
			if err != nil {
				return "", fmt.Errorf("queue %d: %w", i, err)
			}
			if string(m.Body) != "fanout-body" {
				return "", fmt.Errorf("queue %d body %q", i, m.Body)
			}
		}
		return `{"queues_fanned":3}`, nil
	})
}

func topicRouting() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := ch.ExchangeDeclare("compat.topic", "topic", false, false, false, false, nil); err != nil {
			return "", err
		}
		if err := declareQueue(ch, "compat.topic.star"); err != nil {
			return "", err
		}
		if err := declareQueue(ch, "compat.topic.hash"); err != nil {
			return "", err
		}
		if err := ch.QueueBind("compat.topic.star", "stock.*", "compat.topic", false, nil); err != nil {
			return "", err
		}
		if err := ch.QueueBind("compat.topic.hash", "stock.#", "compat.topic", false, nil); err != nil {
			return "", err
		}
		star, err := ch.Consume("compat.topic.star", "", true, false, false, false, nil)
		if err != nil {
			return "", err
		}
		hash, err := ch.Consume("compat.topic.hash", "", true, false, false, false, nil)
		if err != nil {
			return "", err
		}

		if err := publish(ch, "compat.topic", "stock.usd", "one-word"); err != nil {
			return "", err
		}
		if m, err := recvOne(star, 5*time.Second); err != nil || string(m.Body) != "one-word" {
			return "", fmt.Errorf("star one word: %v %q", err, m.Body)
		}
		if m, err := recvOne(hash, 5*time.Second); err != nil || string(m.Body) != "one-word" {
			return "", fmt.Errorf("hash one word: %v %q", err, m.Body)
		}

		if err := publish(ch, "compat.topic", "stock.usd.nyse", "two-words"); err != nil {
			return "", err
		}
		if err := expectNoDelivery(star, 600*time.Millisecond, "topic * must match one word"); err != nil {
			return "", err
		}
		if m, err := recvOne(hash, 5*time.Second); err != nil || string(m.Body) != "two-words" {
			return "", fmt.Errorf("hash two words: %v %q", err, m.Body)
		}
		return `{"star":"one-word","hash":"rest"}`, nil
	})
}

func competingConsumers() (string, error) {
	conn, err := dial()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := declareQueue(ch0(conn), "compat.compete"); err != nil {
		return "", err
	}
	a, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer a.Close()
	b, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer b.Close()

	ma, err := a.Consume("compat.compete", "", false, false, false, false, nil)
	if err != nil {
		return "", err
	}
	mb, err := b.Consume("compat.compete", "", false, false, false, false, nil)
	if err != nil {
		return "", err
	}

	pub, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer pub.Close()
	const n = 10
	for i := 0; i < n; i++ {
		if err := publish(pub, "", "compat.compete", fmt.Sprintf("%d", i)); err != nil {
			return "", err
		}
	}

	seen := map[string]int{}
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < n {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("got %d/%d: %v", len(seen), n, keys(seen))
		}
		got := false
		select {
		case m := <-ma:
			seen[string(m.Body)]++
			_ = m.Ack(false)
			got = true
		case m := <-mb:
			seen[string(m.Body)]++
			_ = m.Ack(false)
			got = true
		case <-time.After(100 * time.Millisecond):
		}
		_ = got
	}
	for k, v := range seen {
		if v != 1 {
			return "", fmt.Errorf("message %s delivered %d times", k, v)
		}
	}
	return fmt.Sprintf(`{"delivered_once":%d}`, n), nil
}

func ch0(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	if err != nil {
		return nil
	}
	return ch
}

func manualAck() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := declareQueue(ch, "compat.ack"); err != nil {
			return "", err
		}
		msgs, err := ch.Consume("compat.ack", "", false, false, false, false, nil)
		if err != nil {
			return "", err
		}
		if err := publish(ch, "", "compat.ack", "ack-me"); err != nil {
			return "", err
		}
		m, err := recvOne(msgs, 5*time.Second)
		if err != nil {
			return "", err
		}
		if err := m.Ack(false); err != nil {
			return "", err
		}
		if err := expectNoDelivery(msgs, 700*time.Millisecond, "acked message redelivered"); err != nil {
			return "", err
		}
		return `{"acked":true,"redelivered":false}`, nil
	})
}

func nackRequeue() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := declareQueue(ch, "compat.nack"); err != nil {
			return "", err
		}
		msgs, err := ch.Consume("compat.nack", "", false, false, false, false, nil)
		if err != nil {
			return "", err
		}
		if err := publish(ch, "", "compat.nack", "retry-me"); err != nil {
			return "", err
		}
		first, err := recvOne(msgs, 5*time.Second)
		if err != nil {
			return "", err
		}
		if err := first.Nack(false, true); err != nil {
			return "", fmt.Errorf("nack: %w", err)
		}
		second, err := recvOne(msgs, 5*time.Second)
		if err != nil {
			return "", fmt.Errorf("requeue: %w", err)
		}
		if string(second.Body) != "retry-me" {
			return "", fmt.Errorf("redelivery body %q", second.Body)
		}
		if err := second.Ack(false); err != nil {
			return "", err
		}
		return `{"nack":"requeued","redelivered":true}`, nil
	})
}

func unbind() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := ch.ExchangeDeclare("compat.unbind", "direct", false, false, false, false, nil); err != nil {
			return "", err
		}
		if err := declareQueue(ch, "compat.unbind.q"); err != nil {
			return "", err
		}
		if err := ch.QueueBind("compat.unbind.q", "k", "compat.unbind", false, nil); err != nil {
			return "", err
		}
		msgs, err := ch.Consume("compat.unbind.q", "", true, false, false, false, nil)
		if err != nil {
			return "", err
		}
		if err := publish(ch, "compat.unbind", "k", "before"); err != nil {
			return "", err
		}
		if m, err := recvOne(msgs, 5*time.Second); err != nil || string(m.Body) != "before" {
			return "", fmt.Errorf("before unbind: %v %q", err, m.Body)
		}
		if err := ch.QueueUnbind("compat.unbind.q", "k", "compat.unbind", nil); err != nil {
			return "", fmt.Errorf("unbind: %w", err)
		}
		if err := publish(ch, "compat.unbind", "k", "after"); err != nil {
			return "", err
		}
		if err := expectNoDelivery(msgs, 700*time.Millisecond, "delivery after unbind"); err != nil {
			return "", err
		}
		return `{"before":"delivered","after":"none"}`, nil
	})
}

func queueDelete() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := declareQueue(ch, "compat.delete"); err != nil {
			return "", err
		}
		n, err := ch.QueueDelete("compat.delete", false, false, false)
		if err != nil {
			return "", fmt.Errorf("delete: %w", err)
		}
		if _, err := ch.QueueDeclarePassive("compat.delete", false, false, false, false, nil); err == nil {
			return "", errors.New("passive declare succeeded after delete")
		}
		return fmt.Sprintf(`{"deleted":true,"messages":%d}`, n), nil
	})
}

func passiveMissing() (string, error) {
	conn, err := dial()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return "", err
	}
	err = ch.ExchangeDeclarePassive("compat.missing.exchange", "direct", false, false, false, false, nil)
	if err == nil {
		return "", errors.New("passive exchange declare of a missing exchange succeeded")
	}
	_ = ch

	ch2, err := conn.Channel()
	if err != nil {
		return "", err
	}
	_, err = ch2.QueueDeclarePassive("compat.missing.queue", false, false, false, false, nil)
	if err == nil {
		return "", errors.New("passive queue declare of a missing queue succeeded")
	}
	return `{"missing_exchange":"channel-error","missing_queue":"channel-error"}`, nil
}

func prefetchQos() (string, error) {
	return withConn(func(_ *amqp.Connection, ch *amqp.Channel) (string, error) {
		if err := declareQueue(ch, "compat.qos"); err != nil {
			return "", err
		}
		if err := ch.Qos(1, 0, false); err != nil {
			return "", fmt.Errorf("qos: %w", err)
		}
		msgs, err := ch.Consume("compat.qos", "", false, false, false, false, nil)
		if err != nil {
			return "", err
		}
		for i := 0; i < 3; i++ {
			if err := publish(ch, "", "compat.qos", fmt.Sprintf("q%d", i)); err != nil {
				return "", err
			}
		}
		first, err := recvOne(msgs, 5*time.Second)
		if err != nil {
			return "", err
		}
		// With prefetch=1 and no ack yet, a second delivery must not arrive.
		if err := expectNoDelivery(msgs, 700*time.Millisecond, "prefetch=1 allowed more than one unacked"); err != nil {
			return "", err
		}
		if err := first.Ack(false); err != nil {
			return "", err
		}
		if _, err := recvOne(msgs, 5*time.Second); err != nil {
			return "", fmt.Errorf("after ack: %w", err)
		}
		return `{"prefetch":1,"unacked":"one-at-a-time"}`, nil
	})
}

func multiChannel() (string, error) {
	conn, err := dial()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	a, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer a.Close()
	b, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer b.Close()

	if err := declareQueue(a, "compat.mc.a"); err != nil {
		return "", err
	}
	if err := declareQueue(b, "compat.mc.b"); err != nil {
		return "", err
	}
	ma, err := a.Consume("compat.mc.a", "", true, false, false, false, nil)
	if err != nil {
		return "", err
	}
	mb, err := b.Consume("compat.mc.b", "", true, false, false, false, nil)
	if err != nil {
		return "", err
	}
	// Interleave publishes across two channels.
	if err := publish(a, "", "compat.mc.a", "via-a"); err != nil {
		return "", err
	}
	if err := publish(b, "", "compat.mc.b", "via-b"); err != nil {
		return "", err
	}
	if m, err := recvOne(ma, 5*time.Second); err != nil || string(m.Body) != "via-a" {
		return "", fmt.Errorf("channel a: %v %q", err, m.Body)
	}
	if m, err := recvOne(mb, 5*time.Second); err != nil || string(m.Body) != "via-b" {
		return "", fmt.Errorf("channel b: %v %q", err, m.Body)
	}
	return `{"channels":2,"isolated":true}`, nil
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
