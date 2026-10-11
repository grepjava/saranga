// Phase 7 exit check: a message crosses from every protocol to every
// other one. Each sender publishes text and a string property `prop` to a
// topic each receiver subscribed to first; the receiver checks the body,
// the property where its protocol carries one, and whether the message
// arrived as text or as bytes.
//   Core JMS, Qpid JMS (AMQP 1.0), the ActiveMQ 5 client (OpenWire), raw
//   STOMP 1.2 and Paho (MQTT 5).
//   java -cp '<artemis>/lib/*;<paho mqttv5 jar>' Conversion.java tcp://host:port [sender-receiver...]
// The broker runs tests/interop/conversion/broker.xml:
//   CONFIG=tests/interop/conversion/broker.xml SUITE_FILE=tests/interop/conversion/Conversion.java SUITE_CP='<artemis lib>/*;<paho mqttv5 jar>' scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

public class Conversion {
    static String url;
    static String host;
    static int port;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int counter;

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        URI u = new URI(url);
        host = u.getHost();
        port = u.getPort();
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        List<Protocol> all = List.of(new Core(), new Amqp(), new OpenWire(), new Stomp(), new Mqtt());
        int failed = 0;
        for (Protocol from : all) {
            for (Protocol to : all) {
                String name = from.name() + "-" + to.name();
                if (!only.isEmpty() && !only.contains(name)) continue;
                long start = System.nanoTime();
                try {
                    cross(from, to);
                    System.out.printf("PASS %-40s %5d ms%n", name, (System.nanoTime() - start) / 1_000_000);
                } catch (Throwable e) {
                    failed++;
                    System.out.printf("FAIL %-40s %s%n", name, e);
                }
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static void eq(Object want, Object got, String what) {
        if (!Objects.equals(want, got)) throw new AssertionError(what + ": want <" + want + "> got <" + got + ">");
    }

    /** What a receiver got: the body as text, the property, and its kind. */
    record Got(String body, String prop, String kind) {}

    interface Receiver extends AutoCloseable {
        Got receive() throws Exception;
    }

    interface Protocol {
        String name();

        /** Subscribes to a topic; messages sent from now on arrive. */
        Receiver subscribe(String topic) throws Exception;

        void send(String topic, String body, String prop) throws Exception;
    }

    static void cross(Protocol from, Protocol to) throws Exception {
        String topic = "conv_" + RUN + "_" + (++counter);
        String body = "from " + from.name() + " to " + to.name() + " é";
        String prop = "value of " + from.name();
        try (Receiver r = to.subscribe(topic)) {
            from.send(topic, body, prop);
            Got got = r.receive();
            check(got != null, "nothing received");
            eq(body, got.body(), "body");
            // MQTT user properties cross only to MQTT: Artemis keeps them
            // apart from the message's own properties.
            boolean mqtt = from instanceof Mqtt || to instanceof Mqtt;
            if (!mqtt || from instanceof Mqtt && to instanceof Mqtt) {
                eq(prop, got.prop(), "property");
            }
            // A JMS receiver sees an MQTT payload as bytes, the rest as text.
            if (to instanceof Jms) {
                eq(from instanceof Mqtt ? "bytes" : "text", got.kind(), "kind");
            }
        }
    }

    // ---------------------------------------------------------------- JMS

    /** A JMS client: Core, Qpid JMS or the ActiveMQ 5 client. */
    abstract static class Jms implements Protocol {
        abstract ConnectionFactory factory();

        Topic topic(Session s, String name) throws JMSException {
            return s.createTopic(name);
        }

        public Receiver subscribe(String name) throws Exception {
            javax.jms.Connection c = factory().createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(topic(s, name));
            return new Receiver() {
                public Got receive() throws Exception {
                    javax.jms.Message m = consumer.receive(5000);
                    if (m == null) return null;
                    String prop = m.getStringProperty("prop");
                    if (m instanceof TextMessage t) return new Got(t.getText(), prop, "text");
                    if (m instanceof BytesMessage b) {
                        byte[] d = new byte[(int) b.getBodyLength()];
                        b.readBytes(d);
                        return new Got(new String(d, StandardCharsets.UTF_8), prop, "bytes");
                    }
                    return new Got(null, prop, m.getClass().getSimpleName());
                }

                public void close() throws Exception {
                    c.close();
                }
            };
        }

        public void send(String name, String body, String prop) throws Exception {
            try (javax.jms.Connection c = factory().createConnection()) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                TextMessage m = s.createTextMessage(body);
                m.setStringProperty("prop", prop);
                s.createProducer(topic(s, name)).send(m);
            }
        }
    }

    static final class Core extends Jms {
        public String name() { return "core"; }

        ConnectionFactory factory() {
            return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(url);
        }
    }

    static final class Amqp extends Jms {
        public String name() { return "amqp"; }

        ConnectionFactory factory() {
            return new org.apache.qpid.jms.JmsConnectionFactory("amqp://" + host + ":" + port);
        }
    }

    static final class OpenWire extends Jms {
        public String name() { return "openwire"; }

        ConnectionFactory factory() {
            return new org.apache.activemq.ActiveMQConnectionFactory("tcp://" + host + ":" + port);
        }
    }

    // ---------------------------------------------------------------- STOMP

    static final class Stomp implements Protocol {
        public String name() { return "stomp"; }

        static final class Conn implements AutoCloseable {
            final Socket socket;
            final InputStream in;
            final OutputStream out;

            Conn() throws IOException {
                socket = new Socket(host, port);
                socket.setSoTimeout(5000);
                in = new BufferedInputStream(socket.getInputStream());
                out = socket.getOutputStream();
                send("CONNECT\naccept-version:1.2\nhost:localhost\n\n");
                check(frame().startsWith("CONNECTED"), "not connected");
            }

            void send(String frame) throws IOException {
                out.write((frame + "\0").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }

            String frame() throws IOException {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                int c;
                while ((c = in.read()) != 0) {
                    if (c == -1) throw new EOFException();
                    if (b.size() == 0 && (c == '\n' || c == '\r')) continue;
                    b.write(c);
                }
                return b.toString(StandardCharsets.UTF_8);
            }

            public void close() throws IOException {
                socket.close();
            }
        }

        public Receiver subscribe(String topic) throws Exception {
            Conn c = new Conn();
            c.send("SUBSCRIBE\nid:1\ndestination:" + topic + "\nsubscription-type:MULTICAST\nreceipt:r\n\n");
            check(c.frame().startsWith("RECEIPT"), "no receipt");
            return new Receiver() {
                public Got receive() throws Exception {
                    String f = c.frame();
                    check(f.startsWith("MESSAGE\n"), "frame " + f);
                    int at = f.indexOf("\n\n");
                    String prop = null;
                    for (String h : f.substring(0, at).split("\n")) {
                        if (h.startsWith("prop:")) prop = h.substring(5).replace("\\c", ":").replace("\\\\", "\\");
                    }
                    return new Got(f.substring(at + 2), prop, "frame");
                }

                public void close() throws Exception {
                    c.close();
                }
            };
        }

        public void send(String topic, String body, String prop) throws Exception {
            try (Conn c = new Conn()) {
                c.send("SEND\ndestination:" + topic + "\ndestination-type:MULTICAST\nprop:" + prop.replace(":", "\\c")
                    + "\nreceipt:s\n\n" + body);
                check(c.frame().startsWith("RECEIPT"), "no receipt");
            }
        }
    }

    // ---------------------------------------------------------------- MQTT

    static final class Mqtt implements Protocol {
        public String name() { return "mqtt"; }

        static MqttAsyncClient client(BlockingQueue<org.eclipse.paho.mqttv5.common.MqttMessage> received) throws Exception {
            MqttAsyncClient client = new MqttAsyncClient(url, "conv-" + RUN + "-" + (++counter), new MemoryPersistence());
            client.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
                public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse r) {}
                public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException e) {}
                public void messageArrived(String topic, org.eclipse.paho.mqttv5.common.MqttMessage m) { received.add(m); }
                public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {}
                public void connectComplete(boolean reconnect, String serverURI) {}
                public void authPacketArrived(int reasonCode, MqttProperties properties) {}
            });
            client.connect(new MqttConnectionOptionsBuilder().cleanStart(true).build()).waitForCompletion(5000);
            return client;
        }

        public Receiver subscribe(String topic) throws Exception {
            BlockingQueue<org.eclipse.paho.mqttv5.common.MqttMessage> received = new LinkedBlockingQueue<>();
            MqttAsyncClient client = client(received);
            client.subscribe(new MqttSubscription(topic, 1)).waitForCompletion(5000);
            return new Receiver() {
                public Got receive() throws Exception {
                    org.eclipse.paho.mqttv5.common.MqttMessage m = received.poll(5, TimeUnit.SECONDS);
                    if (m == null) return null;
                    String prop = null;
                    for (UserProperty p : m.getProperties().getUserProperties()) {
                        if (p.getKey().equals("prop")) prop = p.getValue();
                    }
                    return new Got(new String(m.getPayload(), StandardCharsets.UTF_8), prop, "bytes");
                }

                public void close() throws Exception {
                    client.disconnect().waitForCompletion(5000);
                    client.close();
                }
            };
        }

        public void send(String topic, String body, String prop) throws Exception {
            MqttAsyncClient client = client(new LinkedBlockingQueue<>());
            try {
                org.eclipse.paho.mqttv5.common.MqttMessage m =
                    new org.eclipse.paho.mqttv5.common.MqttMessage(body.getBytes(StandardCharsets.UTF_8));
                m.setQos(1);
                MqttProperties props = new MqttProperties();
                props.setUserProperties(List.of(new UserProperty("prop", prop)));
                m.setProperties(props);
                client.publish(topic, m).waitForCompletion(5000);
            } finally {
                client.disconnect().waitForCompletion(5000);
                client.close();
            }
        }
    }
}
