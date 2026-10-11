// Phase 7 interop suite: MQTT 3.1, 3.1.1 and 5. Paho clients check
// behaviour, a raw socket checks bytes, and JMS (Core) checks what crosses
// between the protocols.
//   java -cp '<artemis>/lib/*;<paho mqttv3>;<paho mqttv5>' Mqtt.java tcp://host:port [test-name...]
// The broker runs tests/interop/mqtt/broker.xml:
//   CONFIG=tests/interop/mqtt/broker.xml SUITE_FILE=tests/interop/mqtt/Mqtt.java SUITE_CP=... scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

public class Mqtt {
    static String url;
    static String host;
    static int port;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int counter;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        URI u = new URI(url);
        host = u.getHost();
        port = u.getPort();
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : Mqtt.class.getDeclaredMethods()) {
            if (m.getName().startsWith("t_")) {
                tests.put(m.getName().substring(2), () -> {
                    try {
                        m.invoke(null);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        if (e.getCause() instanceof Exception ex) throw ex;
                        if (e.getCause() instanceof Error er) throw er;
                        throw e;
                    }
                });
            }
        }
        List<String> names = new ArrayList<>(tests.keySet());
        Collections.sort(names);
        int failed = 0;
        for (String name : names) {
            if (!only.isEmpty() && !only.contains(name)) continue;
            long start = System.nanoTime();
            try {
                tests.get(name).run();
                System.out.printf("PASS %-40s %5d ms%n", name, (System.nanoTime() - start) / 1_000_000);
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-40s %s%n", name, e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static String unique(String base) {
        return base + "-" + RUN + "-" + (++counter);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void eq(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x & 0xff));
        return sb.toString();
    }

    // ------------------------------------------------------------ Paho 3

    /** A received message with its topic. */
    record Got(String topic, MqttMessage message) {}

    static class V3 implements AutoCloseable {
        final MqttClient client;
        final BlockingQueue<Got> received = new LinkedBlockingQueue<>();
        volatile Throwable lost;

        V3(String clientId, boolean clean) throws Exception {
            this(clientId, opts(clean));
        }

        V3(String clientId, MqttConnectOptions options) throws Exception {
            client = new MqttClient(url, clientId, new MemoryPersistence());
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) { lost = cause; }
                public void messageArrived(String topic, MqttMessage message) { received.add(new Got(topic, message)); }
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });
            client.connect(options);
        }

        static MqttConnectOptions opts(boolean clean) {
            MqttConnectOptions o = new MqttConnectOptions();
            o.setCleanSession(clean);
            o.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
            o.setAutomaticReconnect(false);
            return o;
        }

        void publish(String topic, String payload, int qos, boolean retain) throws Exception {
            client.publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, retain);
        }

        void subscribe(String filter, int qos) throws Exception {
            client.subscribe(filter, qos);
        }

        Got next(long ms) throws InterruptedException {
            return received.poll(ms, TimeUnit.MILLISECONDS);
        }

        Got expect() throws InterruptedException {
            Got g = next(5000);
            check(g != null, "no message within 5 s");
            return g;
        }

        void none() throws InterruptedException {
            Got g = next(500);
            check(g == null, "unexpected message " + (g == null ? "" : g.topic() + " " + new String(g.message().getPayload())));
        }

        @Override
        public void close() {
            try {
                if (client.isConnected()) client.disconnect(1000);
            } catch (Exception ignored) {
            }
            try {
                client.close(true);
            } catch (Exception ignored) {
            }
        }
    }

    static String text(Got g) {
        return new String(g.message().getPayload(), StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------ Paho 5

    record Got5(String topic, org.eclipse.paho.mqttv5.common.MqttMessage message) {}

    static class V5 implements AutoCloseable {
        final org.eclipse.paho.mqttv5.client.MqttAsyncClient client;
        final BlockingQueue<Got5> received = new LinkedBlockingQueue<>();
        volatile org.eclipse.paho.mqttv5.client.MqttDisconnectResponse disconnected;

        V5(String clientId, MqttConnectionOptions options) throws Exception {
            client = new org.eclipse.paho.mqttv5.client.MqttAsyncClient(url, clientId,
                    new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
            client.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
                public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse r) { disconnected = r; }
                public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException e) {}
                public void messageArrived(String topic, org.eclipse.paho.mqttv5.common.MqttMessage m) { received.add(new Got5(topic, m)); }
                public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {}
                public void connectComplete(boolean reconnect, String serverURI) {}
                public void authPacketArrived(int reasonCode, MqttProperties properties) {}
            });
            client.connect(options).waitForCompletion(5000);
        }

        void subscribe(MqttSubscription[] subs, MqttProperties props) throws Exception {
            client.subscribe(subs, null, null, props).waitForCompletion(5000);
        }

        V5(String clientId) throws Exception {
            this(clientId, new MqttConnectionOptionsBuilder().cleanStart(true).build());
        }

        Got5 expect() throws InterruptedException {
            Got5 g = received.poll(5000, TimeUnit.MILLISECONDS);
            check(g != null, "no message within 5 s");
            return g;
        }

        void none() throws InterruptedException {
            Got5 g = received.poll(500, TimeUnit.MILLISECONDS);
            check(g == null, "unexpected message on " + (g == null ? "" : g.topic()));
        }

        void publish(String topic, String payload, int qos, boolean retain, MqttProperties props) throws Exception {
            org.eclipse.paho.mqttv5.common.MqttMessage m =
                    new org.eclipse.paho.mqttv5.common.MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            m.setQos(qos);
            m.setRetained(retain);
            if (props != null) m.setProperties(props);
            client.publish(topic, m).waitForCompletion(5000);
        }

        @Override
        public void close() {
            try {
                if (client.isConnected()) client.disconnect(1000).waitForCompletion(2000);
            } catch (Exception ignored) {
            }
            try {
                client.close(true);
            } catch (Exception ignored) {
            }
        }
    }

    // ------------------------------------------------------------ raw

    static class Raw implements AutoCloseable {
        final Socket socket;
        final InputStream in;
        final OutputStream out;

        Raw() throws IOException {
            socket = new Socket(host, port);
            socket.setSoTimeout(5000);
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }

        void send(byte[] b) throws IOException {
            out.write(b);
            out.flush();
        }

        /** The next packet, or null when the connection closed. */
        byte[] packet() throws IOException {
            int b1 = in.read();
            if (b1 < 0) return null;
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            o.write(b1);
            int len = 0, mult = 1, d;
            do {
                d = in.read();
                if (d < 0) return null;
                o.write(d);
                len += (d & 127) * mult;
                mult *= 128;
            } while ((d & 128) != 0);
            byte[] rest = in.readNBytes(len);
            o.write(rest);
            return o.toByteArray();
        }

        /** Whether the broker closes the connection within the time. */
        boolean closedWithin(int ms) throws IOException {
            socket.setSoTimeout(ms);
            try {
                while (true) {
                    if (in.read() < 0) return true;
                }
            } catch (SocketTimeoutException e) {
                return false;
            } catch (SocketException e) {
                return true;
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    static byte[] bytes(int... v) {
        byte[] b = new byte[v.length];
        for (int i = 0; i < v.length; i++) b[i] = (byte) v[i];
        return b;
    }

    static byte[] str(String s) {
        byte[] u = s.getBytes(StandardCharsets.UTF_8);
        byte[] b = new byte[u.length + 2];
        b[0] = (byte) (u.length >> 8);
        b[1] = (byte) u.length;
        System.arraycopy(u, 0, b, 2, u.length);
        return b;
    }

    static byte[] packet(int byte1, byte[]... parts) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] p : parts) body.writeBytes(p);
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(byte1);
        int n = body.size();
        do {
            int d = n % 128;
            n /= 128;
            if (n > 0) d |= 0x80;
            o.write(d);
        } while (n > 0);
        o.writeBytes(body.toByteArray());
        return o.toByteArray();
    }

    /** CONNECT 3.1.1 with a keep-alive and flags. */
    static byte[] connect311(String clientId, int flags, int keepAlive, byte[]... rest) {
        byte[][] parts = new byte[3 + rest.length][];
        parts[0] = bytes(0, 4, 'M', 'Q', 'T', 'T', 4, flags, keepAlive >> 8, keepAlive & 0xff);
        parts[1] = str(clientId);
        parts[2] = new byte[0];
        System.arraycopy(rest, 0, parts, 3, rest.length);
        return packet(0x10, parts);
    }

    static byte[] connect5(String clientId, int flags, byte[] properties) {
        return packet(0x10, bytes(0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 60), properties, str(clientId));
    }

    // ------------------------------------------------------------ JMS

    static javax.jms.Connection jms() throws JMSException {
        javax.jms.Connection c = new ActiveMQConnectionFactory(url).createConnection();
        c.start();
        return c;
    }

    // ============================================================ tests: Paho 3.1.1

    static void t_qos0_1_2_round_trip() throws Exception {
        String topic = unique("round/trip");
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(topic, 2);
            for (int qos = 0; qos <= 2; qos++) {
                pub.publish(topic, "qos" + qos, qos, false);
                Got g = sub.expect();
                eq(topic, g.topic(), "topic");
                eq("qos" + qos, text(g), "payload");
                eq(qos, g.message().getQos(), "qos");
                check(!g.message().isRetained(), "not retained");
            }
        }
    }

    static void t_subscription_qos_downgrades() throws Exception {
        String topic = unique("down");
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(topic, 1);
            pub.publish(topic, "x", 2, false);
            eq(1, sub.expect().message().getQos(), "qos");
            pub.publish(topic, "y", 0, false);
            eq(0, sub.expect().message().getQos(), "qos");
        }
    }

    static void t_wildcards() throws Exception {
        String base = unique("wild");
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(base + "/+/b", 1);
            sub.subscribe(base + "/x/#", 1);
            pub.publish(base + "/a/b", "1", 1, false);
            eq(base + "/a/b", sub.expect().topic(), "single-level");
            pub.publish(base + "/x/y/z", "2", 1, false);
            eq(base + "/x/y/z", sub.expect().topic(), "multi-level");
            pub.publish(base + "/a/c", "3", 1, false);
            sub.none();
        }
    }

    static void t_dots_in_topics() throws Exception {
        // A '.' is a word separator for Core, not for MQTT.
        String base = unique("dot");
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(base + "/a.b", 1);
            pub.publish(base + "/a/b", "no", 1, false);
            pub.publish(base + "/a.b", "yes", 1, false);
            Got g = sub.expect();
            eq(base + "/a.b", g.topic(), "topic");
            eq("yes", text(g), "payload");
            sub.none();
        }
    }

    static void t_retained() throws Exception {
        String topic = unique("retain");
        try (V3 pub = new V3(unique("pub"), true)) {
            pub.publish(topic, "first", 1, true);
            pub.publish(topic, "kept", 1, true);
        }
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            Got g = sub.expect();
            eq("kept", text(g), "the last retained");
            check(g.message().isRetained(), "retain flag");
            sub.none();
        }
        try (V3 pub = new V3(unique("pub"), true)) {
            pub.publish(topic, "", 1, true);
        }
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            sub.none();
        }
    }

    static void t_retained_wildcard_and_live_flag() throws Exception {
        String base = unique("rwild");
        try (V3 pub = new V3(unique("pub"), true)) {
            pub.publish(base + "/a", "A", 0, true);
            pub.publish(base + "/b", "B", 0, true);
        }
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(base + "/#", 1);
            Set<String> got = new HashSet<>();
            got.add(text(sub.expect()));
            got.add(text(sub.expect()));
            eq(Set.of("A", "B"), got, "retained");
            // A live retained publish reaches 3.1.1 subscribers without the flag.
            pub.publish(base + "/c", "C", 1, true);
            Got g = sub.expect();
            eq("C", text(g), "live");
            check(!g.message().isRetained(), "live message is not flagged retained");
            for (String t : List.of("a", "b", "c")) pub.publish(base + "/" + t, "", 0, true);
        }
    }

    static void t_persistent_session() throws Exception {
        String id = unique("persist");
        String topic = unique("persist/topic");
        try (V3 sub = new V3(id, false)) {
            sub.subscribe(topic, 1);
        }
        try (V3 pub = new V3(unique("pub"), true)) {
            pub.publish(topic, "while away", 1, false);
            pub.publish(topic, "qos0 too", 0, false);
        }
        MqttConnectOptions o = V3.opts(false);
        try (V3 sub = new V3(id, o)) {
            check(sub.client.isConnected(), "connected");
            Set<String> got = new HashSet<>();
            got.add(text(sub.expect()));
            got.add(text(sub.expect()));
            eq(Set.of("while away", "qos0 too"), got, "queued");
        }
        // A clean session drops it.
        try (V3 sub = new V3(id, true)) {
        }
        try (V3 pub = new V3(unique("pub"), true)) {
            pub.publish(topic, "nobody", 1, false);
        }
        try (V3 sub = new V3(id, false)) {
            sub.none();
        }
        try (V3 sub = new V3(id, true)) {
        }
    }

    static void t_session_present() throws Exception {
        String id = unique("present");
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x00, 60));
            eq("20020000", hex(r.packet()), "CONNACK for a new session");
            r.send(bytes(0xE0, 0));
        }
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x00, 60));
            eq("20020100", hex(r.packet()), "CONNACK with a session present");
            r.send(bytes(0xE0, 0));
        }
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x02, 60));
            eq("20020000", hex(r.packet()), "clean session");
            r.send(bytes(0xE0, 0));
        }
    }

    static void t_unsubscribe() throws Exception {
        String topic = unique("unsub");
        try (V3 sub = new V3(unique("sub"), true); V3 pub = new V3(unique("pub"), true)) {
            sub.subscribe(topic, 1);
            pub.publish(topic, "1", 1, false);
            sub.expect();
            sub.client.unsubscribe(topic);
            pub.publish(topic, "2", 1, false);
            sub.none();
        }
    }

    static void t_will_on_failure() throws Exception {
        String topic = unique("will");
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            // CONNECT with a will (QoS 1), then the socket just closes.
            Raw r = new Raw();
            r.send(connect311(unique("dying"), 0x02 | 0x04 | 0x08, 60, str(topic), str("goodbye")));
            eq("20020000", hex(r.packet()), "CONNACK");
            r.close();
            Got g = sub.expect();
            eq("goodbye", text(g), "will");
            eq(1, g.message().getQos(), "will qos");
        }
    }

    static void t_no_will_on_disconnect() throws Exception {
        String topic = unique("nowill");
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            try (Raw r = new Raw()) {
                r.send(connect311(unique("polite"), 0x02 | 0x04, 60, str(topic), str("goodbye")));
                eq("20020000", hex(r.packet()), "CONNACK");
                r.send(bytes(0xE0, 0));
                check(r.closedWithin(2000), "closed after DISCONNECT");
            }
            sub.none();
        }
    }

    static void t_will_on_keep_alive_timeout() throws Exception {
        String topic = unique("ka");
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 0);
            try (Raw r = new Raw()) {
                r.send(connect311(unique("silent"), 0x02 | 0x04, 1, str(topic), str("timed out")));
                eq("20020000", hex(r.packet()), "CONNACK");
                check(r.closedWithin(5000), "closed after 1.5 s of silence");
            }
            eq("timed out", text(sub.expect()), "will");
        }
    }

    static void t_ping() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect311(unique("ping"), 0x02, 60));
            r.packet();
            r.send(bytes(0xC0, 0));
            eq("d000", hex(r.packet()), "PINGRESP");
        }
    }

    static void t_link_stealing() throws Exception {
        String id = unique("steal");
        try (Raw first = new Raw(); Raw second = new Raw()) {
            first.send(connect311(id, 0x02, 60));
            eq("20020000", hex(first.packet()), "first CONNACK");
            second.send(connect311(id, 0x02, 60));
            eq("20020000", hex(second.packet()), "second CONNACK");
            check(first.closedWithin(2000), "the first connection is closed");
            second.send(bytes(0xC0, 0));
            eq("d000", hex(second.packet()), "the second still works");
        }
    }

    static void t_empty_client_id() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect311("", 0x02, 60));
            eq("20020000", hex(r.packet()), "a clean session gets an ID");
        }
        try (Raw r = new Raw()) {
            r.send(connect311("", 0x00, 60));
            eq("20020002", hex(r.packet()), "identifier rejected");
            check(r.closedWithin(2000), "closed");
        }
    }

    static void t_bad_protocol_level() throws Exception {
        try (Raw r = new Raw()) {
            r.send(packet(0x10, bytes(0, 4, 'M', 'Q', 'T', 'T', 6, 2, 0, 60), str("x")));
            eq(null, r.packet(), "closed without a CONNACK");
        }
    }

    static void t_mqtt31() throws Exception {
        String topic = unique("v31");
        try (V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            try (Raw r = new Raw()) {
                r.send(packet(0x10, bytes(0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3, 2, 0, 60), str("v31-" + RUN)));
                eq("20020000", hex(r.packet()), "CONNACK");
                r.send(packet(0x32, str(topic), bytes(0, 7), "hello".getBytes()));
                eq("40020007", hex(r.packet()), "PUBACK");
            }
            eq("hello", text(sub.expect()), "payload");
        }
        try (Raw r = new Raw()) {
            // 3.1 client IDs are 1 to 23 characters.
            r.send(packet(0x10, bytes(0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3, 2, 0, 60), str("123456789012345678901234")));
            eq(null, r.packet(), "closed without a CONNACK");
        }
    }

    static void t_raw_qos2_flow() throws Exception {
        String topic = unique("q2");
        try (V3 sub = new V3(unique("sub"), true); Raw r = new Raw()) {
            sub.subscribe(topic, 2);
            r.send(connect311(unique("q2pub"), 0x02, 60));
            r.packet();
            byte[] pub = packet(0x34, str(topic), bytes(0, 9), "x".getBytes());
            r.send(pub);
            eq("50020009", hex(r.packet()), "PUBREC");
            // A duplicate before PUBREL is acknowledged, not routed again.
            r.send(packet(0x3C, str(topic), bytes(0, 9), "x".getBytes()));
            eq("50020009", hex(r.packet()), "PUBREC again");
            r.send(bytes(0x62, 2, 0, 9));
            eq("70020009", hex(r.packet()), "PUBCOMP");
            eq("x", text(sub.expect()), "delivered once");
            sub.none();
            // PUBREL for nothing still completes.
            r.send(bytes(0x62, 2, 0, 10));
            eq("7002000a", hex(r.packet()), "PUBCOMP for an unknown packet");
        }
    }

    static void t_raw_subscriber_qos1_redelivery() throws Exception {
        String id = unique("redeliver");
        String topic = unique("redeliver/t");
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x00, 60));
            r.packet();
            r.send(packet(0x82, bytes(0, 1), str(topic), bytes(1)));
            eq("9003000101", hex(r.packet()), "SUBACK");
            try (V3 pub = new V3(unique("pub"), true)) {
                pub.publish(topic, "m", 1, false);
            }
            byte[] p = r.packet();
            eq(0x32, p[0] & 0xff, "QoS 1 PUBLISH without DUP");
        }
        // Not acknowledged: after reconnecting it comes again, as a DUP with
        // the same packet ID.
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x00, 60));
            eq("20020100", hex(r.packet()), "CONNACK");
            byte[] p = r.packet();
            eq(0x3A, p[0] & 0xff, "DUP QoS 1 PUBLISH");
            int topicLen = ((p[2] & 0xff) << 8) | (p[3] & 0xff);
            int packetId = ((p[4 + topicLen] & 0xff) << 8) | (p[5 + topicLen] & 0xff);
            eq(1, packetId, "the same packet ID");
            r.send(bytes(0x40, 2, 0, packetId));
            r.send(bytes(0xE0, 0));
        }
        try (Raw r = new Raw()) {
            r.send(connect311(id, 0x02, 60));
            r.packet();
        }
    }

    static void t_puback_for_nothing() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect311(unique("x"), 0x02, 60));
            r.packet();
            r.send(bytes(0x40, 2, 0, 5));
            r.send(bytes(0xC0, 0));
            eq("d000", hex(r.packet()), "a PUBACK for nothing is ignored");
        }
    }

    static void t_malformed_closes() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect311(unique("bad"), 0x02, 60));
            r.packet();
            r.send(bytes(0x30, 5, 0, 3, 'a', '/', '#'));
            eq(null, r.packet(), "closed");
        }
    }

    // ============================================================ tests: MQTT <-> JMS

    static void t_mqtt_to_jms() throws Exception {
        String topic = unique("tojms");
        String address = topic.replace('/', '.');
        try (javax.jms.Connection c = jms(); V3 pub = new V3(unique("pub"), true)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createTopic(address));
            pub.publish(topic, "from mqtt", 1, false);
            javax.jms.Message m = consumer.receive(5000);
            check(m instanceof BytesMessage, "a BytesMessage: " + m);
            BytesMessage b = (BytesMessage) m;
            byte[] body = new byte[(int) b.getBodyLength()];
            b.readBytes(body);
            eq("from mqtt", new String(body, StandardCharsets.UTF_8), "body");
            eq(1, m.getIntProperty("mqtt.qos.level"), "qos property");
            eq(false, m.getBooleanProperty("mqtt.message.retain"), "retain property");
            check(m.getJMSDeliveryMode() == DeliveryMode.PERSISTENT, "QoS 1 is durable");
        }
    }

    static void t_jms_to_mqtt() throws Exception {
        String topic = unique("fromjms");
        String address = topic.replace('/', '.');
        try (javax.jms.Connection c = jms(); V3 sub = new V3(unique("sub"), true)) {
            sub.subscribe(topic, 1);
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createTopic(address));
            p.send(s.createTextMessage("text from jms"));
            BytesMessage bm = s.createBytesMessage();
            bm.writeBytes(new byte[] {1, 2, 3});
            p.send(bm);
            Got g = sub.expect();
            eq(topic, g.topic(), "topic");
            eq("text from jms", text(g), "text body");
            eq(1, g.message().getQos(), "qos is the subscription's");
            eq("010203", hex(sub.expect().message().getPayload()), "bytes body");
        }
    }

    static void t_management_messages_filtered() throws Exception {
        try (V3 sub = new V3(unique("sub"), true); javax.jms.Connection c = jms()) {
            sub.subscribe("#", 0);
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Messages to $ topics do not match a filter starting with a wildcard.
            try (V3 pub = new V3(unique("pub"), true)) {
                pub.publish("$" + unique("dollar"), "hidden", 0, false);
            }
            sub.none();
        }
    }

    // ============================================================ tests: MQTT 5

    static void t_v5_round_trip_properties() throws Exception {
        String topic = unique("v5props");
        try (V5 sub = new V5(unique("sub")); V5 pub = new V5(unique("pub"))) {
            sub.subscribe(new MqttSubscription[] {new MqttSubscription(topic, 1)}, new MqttProperties());
            MqttProperties props = new MqttProperties();
            props.setContentType("text/plain");
            props.setResponseTopic("reply/here");
            props.setCorrelationData(new byte[] {9, 8, 7});
            props.setPayloadFormat(true);
            props.setUserProperties(List.of(new UserProperty("b", "2"), new UserProperty("a", "1"), new UserProperty("b", "3")));
            props.setMessageExpiryInterval(3600L);
            pub.publish(topic, "props", 1, false, props);
            Got5 g = sub.expect();
            MqttProperties got = g.message().getProperties();
            eq("text/plain", got.getContentType(), "content type");
            eq("reply/here", got.getResponseTopic(), "response topic");
            eq("090807", hex(got.getCorrelationData()), "correlation data");
            eq(true, got.getPayloadFormat(), "payload format");
            List<String> users = new ArrayList<>();
            for (UserProperty u : got.getUserProperties()) users.add(u.getKey() + "=" + u.getValue());
            eq(List.of("b=2", "a=1", "b=3"), users, "user properties in order");
            Long expiry = got.getMessageExpiryInterval();
            check(expiry != null && expiry > 3590 && expiry <= 3600, "expiry " + expiry);
        }
    }

    static void t_v5_subscription_identifier() throws Exception {
        String topic = unique("subid");
        try (V5 sub = new V5(unique("sub")); V5 pub = new V5(unique("pub"))) {
            MqttProperties sp = new MqttProperties();
            sp.setSubscriptionIdentifier(42);
            sub.subscribe(new MqttSubscription[] {new MqttSubscription(topic, 1)}, sp);
            pub.publish(topic, "x", 1, false, null);
            eq(List.of(42), sub.expect().message().getProperties().getSubscriptionIdentifiers(), "subscription ids");
        }
    }

    static void t_v5_no_local() throws Exception {
        String topic = unique("nolocal");
        try (V5 c = new V5(unique("both")); V5 other = new V5(unique("other"))) {
            MqttSubscription s = new MqttSubscription(topic, 1);
            s.setNoLocal(true);
            c.subscribe(new MqttSubscription[] {s}, new MqttProperties());
            c.publish(topic, "mine", 1, false, null);
            c.none();
            other.publish(topic, "theirs", 1, false, null);
            eq("theirs", new String(c.expect().message().getPayload()), "from another client");
        }
    }

    static void t_v5_retain_handling() throws Exception {
        String topic = unique("rh");
        try (V5 pub = new V5(unique("pub"))) {
            pub.publish(topic, "retained", 1, true, null);
        }
        try (V5 sub = new V5(unique("sub"))) {
            MqttSubscription never = new MqttSubscription(topic, 1);
            never.setRetainHandling(2);
            sub.subscribe(new MqttSubscription[] {never}, new MqttProperties());
            sub.none();
        }
        try (V5 sub = new V5(unique("sub"))) {
            MqttSubscription s = new MqttSubscription(topic, 1);
            s.setRetainHandling(1);
            sub.subscribe(new MqttSubscription[] {s}, new MqttProperties());
            check(sub.expect().message().isRetained(), "retained on a new subscription");
            sub.subscribe(new MqttSubscription[] {s}, new MqttProperties());
            sub.none();
        }
        try (V5 pub = new V5(unique("pub"))) {
            pub.publish(topic, "", 1, true, null);
        }
    }

    static void t_v5_retain_as_published() throws Exception {
        String topic = unique("rap");
        try (V5 sub = new V5(unique("sub")); V5 pub = new V5(unique("pub"))) {
            MqttSubscription s = new MqttSubscription(topic, 1);
            s.setRetainAsPublished(true);
            sub.subscribe(new MqttSubscription[] {s}, new MqttProperties());
            pub.publish(topic, "live", 1, true, null);
            check(sub.expect().message().isRetained(), "the retain flag as published");
            pub.publish(topic, "", 1, true, null);
            sub.expect();
        }
    }

    static void t_v5_shared_subscription() throws Exception {
        String topic = unique("shared");
        String filter = "$share/" + unique("g") + "/" + topic;
        try (V5 a = new V5(unique("a")); V5 b = new V5(unique("b")); V5 pub = new V5(unique("pub"))) {
            a.subscribe(new MqttSubscription[] {new MqttSubscription(filter, 1)}, new MqttProperties());
            b.subscribe(new MqttSubscription[] {new MqttSubscription(filter, 1)}, new MqttProperties());
            for (int i = 0; i < 10; i++) pub.publish(topic, "m" + i, 1, false, null);
            int n = 0;
            while (a.received.poll(1000, TimeUnit.MILLISECONDS) != null) n++;
            int m = 0;
            while (b.received.poll(200, TimeUnit.MILLISECONDS) != null) m++;
            eq(10, n + m, "each message once");
            check(n > 0 && m > 0, "shared between the two: " + n + "/" + m);
        }
    }

    static void t_v5_session_expiry() throws Exception {
        String id = unique("expiry");
        String topic = unique("expiry/t");
        MqttConnectionOptions o = new MqttConnectionOptionsBuilder().cleanStart(true).sessionExpiryInterval(1L).build();
        try (V5 c = new V5(id, o)) {
            c.subscribe(new MqttSubscription[] {new MqttSubscription(topic, 1)}, new MqttProperties());
        }
        Thread.sleep(2500);
        try (V5 pub = new V5(unique("pub"))) {
            pub.publish(topic, "late", 1, false, null);
        }
        MqttConnectionOptions again = new MqttConnectionOptionsBuilder().cleanStart(false).sessionExpiryInterval(60L).build();
        try (V5 c = new V5(id, again)) {
            check(!c.client.getCurrentServerURI().isEmpty(), "connected");
            c.none();
        }
        try (V5 c = new V5(id)) {
        }
    }

    static void t_v5_session_kept() throws Exception {
        String id = unique("kept");
        String topic = unique("kept/t");
        MqttConnectionOptions o = new MqttConnectionOptionsBuilder().cleanStart(true).sessionExpiryInterval(60L).build();
        try (V5 c = new V5(id, o)) {
            c.subscribe(new MqttSubscription[] {new MqttSubscription(topic, 1)}, new MqttProperties());
        }
        try (V5 pub = new V5(unique("pub"))) {
            pub.publish(topic, "kept", 1, false, null);
        }
        MqttConnectionOptions again = new MqttConnectionOptionsBuilder().cleanStart(false).sessionExpiryInterval(0L).build();
        try (V5 c = new V5(id, again)) {
            eq("kept", new String(c.expect().message().getPayload()), "queued while away");
        }
    }

    static void t_v5_topic_alias_from_client() throws Exception {
        String topic = unique("alias");
        try (V5 sub = new V5(unique("sub")); Raw r = new Raw()) {
            sub.subscribe(new MqttSubscription[] {new MqttSubscription(topic, 1)}, new MqttProperties());
            r.send(connect5(unique("aliaser"), 0x02, bytes(0)));
            r.packet();
            // PUBLISH with topic and alias 1, then with the alias alone.
            r.send(packet(0x30, str(topic), bytes(3, 0x23, 0, 1), "one".getBytes()));
            r.send(packet(0x30, str(""), bytes(3, 0x23, 0, 1), "two".getBytes()));
            eq("one", new String(sub.expect().message().getPayload()), "first");
            eq("two", new String(sub.expect().message().getPayload()), "by alias");
            // An unknown alias with no topic is a protocol error.
            r.send(packet(0x30, str(""), bytes(3, 0x23, 0, 2), "three".getBytes()));
            eq("e0029400", hex(r.packet()), "DISCONNECT topic alias invalid");
            eq(null, r.packet(), "closed");
        }
    }

    static void t_v5_connack_bytes() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect5(unique("c5"), 0x02, bytes(0)));
            // Topic alias maximum 65535, maximum packet size 268435455.
            eq("200b00000822ffff270fffffff", hex(r.packet()), "CONNACK");
        }
        try (Raw r = new Raw()) {
            r.send(connect5("", 0x02, bytes(0)));
            byte[] p = r.packet();
            eq(0x12, p[5] & 0xff, "assigned client identifier first: " + hex(p));
        }
        try (Raw r = new Raw()) {
            // A keep-alive over the server's 60 s gets the server's.
            r.send(packet(0x10, bytes(0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 120), bytes(0), str(unique("ka"))));
            eq("200e00000b22ffff13003c270fffffff", hex(r.packet()), "server keep alive");
        }
    }

    static void t_v5_auth_method_refused() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect5(unique("auth"), 0x02, bytes(6, 0x15, 0, 3, 'f', 'o', 'o')));
            eq("2003008c00", hex(r.packet()), "bad authentication method");
            eq(null, r.packet(), "closed");
        }
    }

    static void t_v5_malformed() throws Exception {
        try (Raw r = new Raw()) {
            r.send(connect5(unique("mal"), 0x02, bytes(0)));
            r.packet();
            r.send(bytes(0x30, 5, 0, 3, 'a', '/', '+'));
            eq("e0028100", hex(r.packet()), "DISCONNECT malformed packet");
            eq(null, r.packet(), "closed");
        }
    }

    static void t_v5_link_stealing() throws Exception {
        String id = unique("steal5");
        try (Raw first = new Raw(); Raw second = new Raw()) {
            first.send(connect5(id, 0x02, bytes(0)));
            first.packet();
            second.send(connect5(id, 0x02, bytes(0)));
            eq("e0028e00", hex(first.packet()), "session taken over");
            eq(null, first.packet(), "closed");
            check(second.packet()[0] == 0x20, "second CONNACK");
        }
    }

    static void t_v5_unsubscribe_codes() throws Exception {
        String topic = unique("unsub5");
        try (Raw r = new Raw()) {
            r.send(connect5(unique("u5"), 0x02, bytes(0)));
            r.packet();
            r.send(packet(0x82, bytes(0, 1, 0), str(topic), bytes(1)));
            eq("9004000100" + "01", hex(r.packet()), "SUBACK");
            r.send(packet(0xA2, bytes(0, 2, 0), str(topic), str("nothing/here")));
            eq("b00500020000" + "11", hex(r.packet()), "UNSUBACK");
        }
    }

    static void t_v5_receive_maximum() throws Exception {
        String topic = unique("recvmax");
        try (Raw r = new Raw(); V5 pub = new V5(unique("pub"))) {
            r.send(connect5(unique("rm"), 0x02, bytes(3, 0x21, 0, 1)));
            r.packet();
            r.send(packet(0x82, bytes(0, 1, 0), str(topic), bytes(1)));
            r.packet();
            pub.publish(topic, "1", 1, false, null);
            pub.publish(topic, "2", 1, false, null);
            byte[] first = r.packet();
            r.socket.setSoTimeout(500);
            try {
                r.packet();
                throw new AssertionError("a second PUBLISH before the first was acknowledged");
            } catch (SocketTimeoutException expected) {
            }
            r.socket.setSoTimeout(5000);
            int topicLen = ((first[2] & 0xff) << 8) | (first[3] & 0xff);
            r.send(bytes(0x40, 2, first[4 + topicLen], first[5 + topicLen]));
            check((r.packet()[0] & 0xf0) == 0x30, "the second once the first was acknowledged");
        }
    }
}
