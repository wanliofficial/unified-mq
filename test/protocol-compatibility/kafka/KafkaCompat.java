// S5 Kafka real-client compatibility suite.
//
// Drives a real UnifiedMQ Kafka listener with the official Apache
// kafka-clients 3.7.1 library. It never speaks the wire protocol by hand:
// every case goes through KafkaProducer / KafkaConsumer / AdminClient-style
// metadata calls, so a passing run is evidence that a third-party client
// works against this adapter.
//
// Usage (the Python driver supplies the address):
//
//   java KafkaCompat --bootstrap 127.0.0.1:PORT [-case name]
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class KafkaCompat {

    static String bootstrap;
    static final List<Object[]> CASES = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String only = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--bootstrap")) {
                bootstrap = args[++i];
            } else if (args[i].equals("--case")) {
                only = args[++i];
            }
        }
        if (bootstrap == null) {
            System.err.println("--bootstrap is required");
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
            @SuppressWarnings("unchecked")
            Supplier<String> fn = (Supplier<String>) c[1];
            long start = System.nanoTime();
            try {
                String detail = fn.get();
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

    interface Fn {
        String run() throws Exception;
    }

    static void test(String name, Fn fn) {
        CASES.add(new Object[]{name, (Supplier<String>) () -> {
            try {
                return fn.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }});
    }

    static void registerCases() {
        test("kafka-metadata", KafkaCompat::metadata);
        test("kafka-produce-consume", KafkaCompat::produceConsume);
        test("kafka-batch-order", KafkaCompat::batchOrder);
        test("kafka-explicit-partition", KafkaCompat::explicitPartition);
        test("kafka-key-roundtrip", KafkaCompat::keyRoundtrip);
        test("kafka-headers", KafkaCompat::headers);
        test("kafka-timestamp", KafkaCompat::timestamp);
        test("kafka-acks-all-idempotent", KafkaCompat::acksAllIdempotent);
        test("kafka-beginning-end-offsets", KafkaCompat::beginningEndOffsets);
        test("kafka-consumer-group", KafkaCompat::consumerGroup);
        test("kafka-offset-commit", KafkaCompat::offsetCommit);
        test("kafka-seek-replay", KafkaCompat::seekReplay);
    }

    static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return p;
    }

    static Properties consumerProps(String group) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000");
        return p;
    }

    static RecordMetadata send(KafkaProducer<String, String> p, String topic,
                               Integer partition, String key, String value) throws Exception {
        Future<RecordMetadata> f = p.send(new ProducerRecord<>(topic, partition, key, value));
        return f.get(20, TimeUnit.SECONDS);
    }

    static List<ConsumerRecord<String, String>> pollAtLeast(KafkaConsumer<String, String> c,
                                                             int n) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 20000;
        while (out.size() < n && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                out.add(r);
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- cases

    static String metadata() throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        try (Admin admin = Admin.create(p)) {
            List<PartitionInfo> parts = admin.describeTopics(
                    Collections.singletonList("compat.metadata"))
                    .allTopicNames().get(20, TimeUnit.SECONDS)
                    .get("compat.metadata")
                    .partitions()
                    .stream()
                    .map(x -> new PartitionInfo("compat.metadata", x.partition(),
                            x.leader() == null ? null : new org.apache.kafka.common.Node(
                                    x.leader().id(), x.leader().host(), x.leader().port()),
                            new org.apache.kafka.common.Node[0],
                            new org.apache.kafka.common.Node[0],
                            new org.apache.kafka.common.Node[0]))
                    .collect(java.util.stream.Collectors.toList());
            if (parts.size() != 2) {
                throw new IllegalStateException("expected 2 partitions, got " + parts.size());
            }
            return "{\"partitions\":" + parts.size() + "}";
        }
    }

    static String produceConsume() throws Exception {
        String topic = "compat.pubsub";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            RecordMetadata md = send(p, topic, null, null, "hello-kafka");
            if (md.partition() < 0 || md.offset() < 0) {
                throw new IllegalStateException("bad metadata " + md);
            }
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-pubsub"))) {
            c.subscribe(Collections.singletonList(topic));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, 1);
            if (recs.size() != 1 || !recs.get(0).value().equals("hello-kafka")) {
                throw new IllegalStateException("unexpected records " + recs);
            }
            return "{\"value\":\"" + recs.get(0).value() + "\"}";
        }
    }

    static String batchOrder() throws Exception {
        String topic = "compat.batch";
        int n = 20;
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < n; i++) {
                send(p, topic, 0, "k" + i, "v" + i);
            }
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-batch"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, n);
            if (recs.size() != n) {
                throw new IllegalStateException("got " + recs.size() + "/" + n);
            }
            for (int i = 0; i < n; i++) {
                if (!recs.get(i).value().equals("v" + i)
                        || !recs.get(i).key().equals("k" + i)) {
                    throw new IllegalStateException("order broken at " + i + ": " + recs.get(i));
                }
            }
            return "{\"count\":" + n + ",\"ordered\":true}";
        }
    }

    static String explicitPartition() throws Exception {
        String topic = "compat.partition";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            send(p, topic, 1, "k", "on-one");
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-partition"))) {
            TopicPartition tp = new TopicPartition(topic, 1);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, 1);
            if (recs.size() != 1 || recs.get(0).partition() != 1) {
                throw new IllegalStateException("unexpected " + recs);
            }
            return "{\"partition\":" + recs.get(0).partition() + "}";
        }
    }

    static String keyRoundtrip() throws Exception {
        String topic = "compat.key";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            send(p, topic, 0, "the-key", "the-value");
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-key"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, 1);
            if (recs.size() != 1 || !"the-key".equals(recs.get(0).key())) {
                throw new IllegalStateException("key lost: " + recs);
            }
            return "{\"key\":\"" + recs.get(0).key() + "\"}";
        }
    }

    static String headers() throws Exception {
        String topic = "compat.headers";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, 0, "k", "v");
            rec.headers().add(new RecordHeader("trace-id", "abc-123".getBytes()));
            rec.headers().add(new RecordHeader("source", "compat".getBytes()));
            p.send(rec).get(20, TimeUnit.SECONDS);
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-headers"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, 1);
            if (recs.size() != 1) {
                throw new IllegalStateException("no record");
            }
            Map<String, String> got = new HashMap<>();
            for (Header h : recs.get(0).headers()) {
                got.put(h.key(), new String(h.value()));
            }
            if (!"abc-123".equals(got.get("trace-id")) || !"compat".equals(got.get("source"))) {
                throw new IllegalStateException("headers lost: " + got);
            }
            return "{\"headers\":" + got.size() + "}";
        }
    }

    static String timestamp() throws Exception {
        String topic = "compat.timestamp";
        long ts = 1700000000123L;
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            p.send(new ProducerRecord<>(topic, 0, ts, "k", "timed")).get(20, TimeUnit.SECONDS);
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-timestamp"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, 1);
            if (recs.size() != 1 || recs.get(0).timestamp() != ts) {
                throw new IllegalStateException("timestamp " + (recs.isEmpty() ? "none" : recs.get(0).timestamp()));
            }
            return "{\"timestamp\":" + recs.get(0).timestamp() + "}";
        }
    }

    static String acksAllIdempotent() throws Exception {
        String topic = "compat.idempotent";
        Properties p = producerProps();
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        int n = 10;
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            for (int i = 0; i < n; i++) {
                send(prod, topic, 0, null, "idem-" + i);
            }
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-idempotent"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> recs = pollAtLeast(c, n);
            if (recs.size() != n) {
                throw new IllegalStateException("got " + recs.size() + "/" + n);
            }
            return "{\"idempotent\":true,\"count\":" + n + "}";
        }
    }

    static String beginningEndOffsets() throws Exception {
        String topic = "compat.offsets";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < 5; i++) {
                send(p, topic, 0, null, "o" + i);
            }
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-offsets"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            Map<TopicPartition, Long> begin = c.beginningOffsets(Collections.singletonList(tp));
            Map<TopicPartition, Long> end = c.endOffsets(Collections.singletonList(tp));
            long b = begin.get(tp);
            long e = end.get(tp);
            if (b != 0 || e < 5) {
                throw new IllegalStateException("offsets begin=" + b + " end=" + e);
            }
            return "{\"begin\":" + b + ",\"end\":" + e + "}";
        }
    }

    static String consumerGroup() throws Exception {
        String topic = "compat.group";
        int n = 6;
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < n; i++) {
                send(p, topic, i % 2, "k" + i, "g" + i);
            }
        }
        String group = "compat-group-shared";
        KafkaConsumer<String, String> a = new KafkaConsumer<>(consumerProps(group));
        KafkaConsumer<String, String> b = new KafkaConsumer<>(consumerProps(group));
        try {
            a.subscribe(Collections.singletonList(topic));
            b.subscribe(Collections.singletonList(topic));
            Map<String, Integer> seen = new HashMap<>();
            Set<Integer> assigned = new HashSet<>();
            long deadline = System.currentTimeMillis() + 30000;
            while (seen.size() < n && System.currentTimeMillis() < deadline) {
                for (KafkaConsumer<String, String> c : List.of(a, b)) {
                    ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(300));
                    assigned.addAll(c.assignment().stream()
                            .filter(tp -> tp.topic().equals(topic))
                            .map(TopicPartition::partition)
                            .collect(java.util.stream.Collectors.toSet()));
                    for (ConsumerRecord<String, String> r : recs) {
                        seen.merge(r.value(), 1, Integer::sum);
                    }
                }
            }
            if (seen.size() != n) {
                throw new IllegalStateException("got " + seen.size() + "/" + n + " " + seen);
            }
            for (Map.Entry<String, Integer> e : seen.entrySet()) {
                if (e.getValue() != 1) {
                    throw new IllegalStateException(e.getKey() + " delivered " + e.getValue());
                }
            }
            if (assigned.size() != 2) {
                throw new IllegalStateException("assignment did not cover both partitions: " + assigned);
            }
            return "{\"delivered_once\":" + n + ",\"assigned_partitions\":" + assigned.size() + "}";
        } finally {
            a.close();
            b.close();
        }
    }

    static String offsetCommit() throws Exception {
        String topic = "compat.commit";
        int n = 5;
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < n; i++) {
                send(p, topic, 0, null, "c" + i);
            }
        }
        String group = "compat-commit-group";
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> first = pollAtLeast(c, 3);
            if (first.size() < 3) {
                throw new IllegalStateException("only " + first.size());
            }
            long lastOffset = first.get(2).offset();
            c.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(lastOffset + 1)));
        }
        try (KafkaConsumer<String, String> c2 = new KafkaConsumer<>(consumerProps(group))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c2.assign(Collections.singletonList(tp));
            OffsetAndMetadata committed = c2.committed(tp);
            if (committed == null || committed.offset() != 3) {
                throw new IllegalStateException("committed " + committed);
            }
            List<ConsumerRecord<String, String>> rest = pollAtLeast(c2, 2);
            if (rest.size() != 2 || !rest.get(0).value().equals("c3")) {
                throw new IllegalStateException("resume failed " + rest);
            }
            return "{\"committed\":" + committed.offset() + ",\"resumed_at\":\"c3\"}";
        }
    }

    static String seekReplay() throws Exception {
        String topic = "compat.replay";
        try (KafkaProducer<String, String> p = new KafkaProducer<>(producerProps())) {
            for (int i = 0; i < 3; i++) {
                send(p, topic, 0, null, "r" + i);
            }
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("compat-replay"))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singletonList(tp));
            c.seekToBeginning(Collections.singletonList(tp));
            List<ConsumerRecord<String, String>> first = pollAtLeast(c, 3);
            if (first.size() != 3) {
                throw new IllegalStateException("first pass " + first.size());
            }
            c.seek(tp, 0);
            List<ConsumerRecord<String, String>> second = pollAtLeast(c, 3);
            if (second.size() != 3) {
                throw new IllegalStateException("replay " + second.size());
            }
            return "{\"replayed\":" + second.size() + "}";
        }
    }
}
