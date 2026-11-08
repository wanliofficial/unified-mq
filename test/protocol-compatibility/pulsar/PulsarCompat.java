// S5 Pulsar real-client compatibility suite.
//
// Drives a real UnifiedMQ Pulsar listener with the official Apache
// pulsar-client 3.3.1 library. It never speaks the wire protocol by hand:
// every case goes through PulsarClient / Producer / Consumer, so a passing run
// is evidence that the official Java client works against this adapter.
//
// Usage (the Python driver supplies the address):
//
//   java PulsarCompat --service-url pulsar://127.0.0.1:PORT [-case name]
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PulsarCompat {

    static String serviceUrl;
    static final List<Object[]> CASES = new ArrayList<>();

    interface Fn {
        String run() throws Exception;
    }

    static void test(String name, Fn fn) {
        CASES.add(new Object[]{name, fn});
    }

    public static void main(String[] args) throws Exception {
        String only = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--service-url")) {
                serviceUrl = args[++i];
            } else if (args[i].equals("--case")) {
                only = args[++i];
            }
        }
        if (serviceUrl == null) {
            System.err.println("--service-url is required");
            System.exit(2);
        }
        registerCases();
        int passed = 0;
        int failed = 0;
        for (Object[] c : CASES) {
            String name = (String) c[0];
            if (!only.isEmpty() && !only.equals(name)) {
                continue;
            }
            Fn fn = (Fn) c[1];
            long start = System.nanoTime();
            try {
                String detail = fn.run();
                double secs = (System.nanoTime() - start) / 1e9;
                System.out.printf("PASS %s (%.2fs) %s%n", name, secs, detail);
                passed++;
            } catch (Throwable t) {
                double secs = (System.nanoTime() - start) / 1e9;
                System.out.printf("FAIL %s (%.2fs) %s%n", name, secs, t);
                t.printStackTrace(System.out);
                failed++;
            }
        }
        System.out.printf("SUMMARY passed=%d failed=%d%n", passed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    static void registerCases() {
        test("pulsar-produce-consume", PulsarCompat::produceConsume);
        test("pulsar-properties", PulsarCompat::properties);
        test("pulsar-key", PulsarCompat::key);
        test("pulsar-batch", PulsarCompat::batch);
        test("pulsar-ack-redelivery", PulsarCompat::ackRedelivery);
        test("pulsar-seek", PulsarCompat::seek);
        test("pulsar-unsubscribe", PulsarCompat::unsubscribe);
        test("pulsar-producer-name", PulsarCompat::producerName);
    }

    static PulsarClient client() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .operationTimeout(10, TimeUnit.SECONDS)
                .connectionTimeout(10, TimeUnit.SECONDS)
                .build();
    }

    static Consumer<String> consumer(PulsarClient c, String topic, String sub)
            throws Exception {
        ConsumerBuilder<String> b = c.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(sub)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        return b.subscribe();
    }

    // ---------------------------------------------------------------- cases

    static String produceConsume() throws Exception {
        String topic = "compat-pubsub";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create();
             Consumer<String> consumer = consumer(c, topic, "compat-sub")) {
            MessageId id = p.send("hello-pulsar");
            if (id == null) {
                throw new IllegalStateException("null message id");
            }
            Message<String> m = consumer.receive(20, TimeUnit.SECONDS);
            if (m == null || !"hello-pulsar".equals(m.getValue())) {
                throw new IllegalStateException("unexpected message " + m);
            }
            consumer.acknowledge(m);
            return "{\"value\":\"" + m.getValue() + "\"}";
        }
    }

    static String properties() throws Exception {
        String topic = "compat-properties";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create();
             Consumer<String> consumer = consumer(c, topic, "compat-props")) {
            p.newMessage().value("with-props")
                    .property("k1", "v1")
                    .property("k2", "v2")
                    .send();
            Message<String> m = consumer.receive(20, TimeUnit.SECONDS);
            if (m == null || !"v1".equals(m.getProperty("k1"))
                    || !"v2".equals(m.getProperty("k2"))) {
                throw new IllegalStateException("properties did not round-trip: " + m);
            }
            consumer.acknowledge(m);
            return "{\"k1\":\"" + m.getProperty("k1") + "\"}";
        }
    }

    static String key() throws Exception {
        String topic = "compat-key";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create();
             Consumer<String> consumer = consumer(c, topic, "compat-key")) {
            p.newMessage().value("keyed").key("my-key").send();
            Message<String> m = consumer.receive(20, TimeUnit.SECONDS);
            if (m == null || !"my-key".equals(m.getKey())) {
                throw new IllegalStateException("key did not round-trip: " + m);
            }
            consumer.acknowledge(m);
            return "{\"key\":\"" + m.getKey() + "\"}";
        }
    }

    static String batch() throws Exception {
        String topic = "compat-batch";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING)
                     .topic(topic)
                     .batchingMaxMessages(10)
                     .enableBatching(true)
                     .create();
             Consumer<String> consumer = consumer(c, topic, "compat-batch")) {
            p.newMessage().value("b0").sendAsync();
            p.newMessage().value("b1").sendAsync();
            p.newMessage().value("b2").sendAsync();
            p.flush();
            List<String> got = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 20000;
            while (got.size() < 3 && System.currentTimeMillis() < deadline) {
                Message<String> m = consumer.receive(1000, TimeUnit.MILLISECONDS);
                if (m != null) {
                    got.add(m.getValue());
                    consumer.acknowledge(m);
                }
            }
            if (!got.equals(List.of("b0", "b1", "b2"))) {
                throw new IllegalStateException("unexpected batch " + got);
            }
            return "{\"values\":" + got + "}";
        }
    }

    static String ackRedelivery() throws Exception {
        String topic = "compat-redelivery";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create()) {
            p.send("redeliver-me");
            // First consumer receives without acking, then closes.
            try (Consumer<String> first = consumer(c, topic, "compat-redelivery-sub")) {
                Message<String> m = first.receive(20, TimeUnit.SECONDS);
                if (m == null || !"redeliver-me".equals(m.getValue())) {
                    throw new IllegalStateException("first receive failed: " + m);
                }
            }
            // Second consumer on the same durable subscription must see it again.
            try (Consumer<String> second = consumer(c, topic, "compat-redelivery-sub")) {
                Message<String> m = second.receive(20, TimeUnit.SECONDS);
                if (m == null || !"redeliver-me".equals(m.getValue())) {
                    throw new IllegalStateException("no redelivery: " + m);
                }
                second.acknowledge(m);
                return "{\"value\":\"" + m.getValue() + "\"}";
            }
        }
    }

    static String seek() throws Exception {
        String topic = "compat-seek";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create();
             Consumer<String> consumer = consumer(c, topic, "compat-seek")) {
            p.send("s0");
            p.send("s1");
            Message<String> m0 = consumer.receive(20, TimeUnit.SECONDS);
            Message<String> m1 = consumer.receive(20, TimeUnit.SECONDS);
            if (m0 == null || m1 == null) {
                throw new IllegalStateException("did not receive both");
            }
            consumer.acknowledge(m0);
            consumer.acknowledge(m1);
            consumer.seek(MessageId.earliest);
            Message<String> again = consumer.receive(20, TimeUnit.SECONDS);
            if (again == null || !"s0".equals(again.getValue())) {
                throw new IllegalStateException("seek did not replay: " + again);
            }
            consumer.acknowledge(again);
            return "{\"value\":\"" + again.getValue() + "\"}";
        }
    }

    static String unsubscribe() throws Exception {
        String topic = "compat-unsubscribe";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING).topic(topic).create();
             Consumer<String> consumer = consumer(c, topic, "compat-unsub")) {
            p.send("u0");
            Message<String> m = consumer.receive(20, TimeUnit.SECONDS);
            if (m == null) {
                throw new IllegalStateException("nothing to receive");
            }
            consumer.unsubscribe();
            return "{\"status\":\"unsubscribed\"}";
        }
    }

    static String producerName() throws Exception {
        String topic = "compat-producer-name";
        try (PulsarClient c = client();
             Producer<String> p = c.newProducer(Schema.STRING)
                     .topic(topic)
                     .producerName("compat-named-producer")
                     .create()) {
            if (!"compat-named-producer".equals(p.getProducerName())) {
                throw new IllegalStateException("producer name not honoured: "
                        + p.getProducerName());
            }
            p.send("named");
            return "{\"name\":\"" + p.getProducerName() + "\"}";
        }
    }
}
