// Phase 7 interop suite: every protocol over WebSockets on the acceptor
// that serves it over TCP, as Artemis's ProtocolHandler and
// WebSocketServerHandler do it (ported in part from Artemis's
// WebSocketTest, StompWebSocketMaxFrameTest and MQTTWebSocketTest).
// Paho (ws://) and Qpid JMS (amqpws://) are real WebSocket clients; STOMP
// and OpenWire go through the JDK's WebSocket client or a raw socket,
// which also checks the handshake and the framing byte for byte.
//   java -cp '<artemis>/lib/*;<paho mqttv3 jar>' WebSocket.java tcp://host:port [test-name...]
// The broker runs tests/interop/websocket/broker.xml:
//   CONFIG=tests/interop/websocket/broker.xml SUITE_FILE=tests/interop/websocket/WebSocket.java SUITE_CP='<artemis lib>/*;<paho mqttv3 jar>' scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;

import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class WebSocket {
    static String url;
    static String host;
    static int port;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int counter;
    /** The acceptor's webSocketMaxFramePayloadLength. */
    static final int MAX_PAYLOAD = 2048;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        URI u = new URI(url);
        host = u.getHost();
        port = u.getPort();
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : WebSocket.class.getDeclaredMethods()) {
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

    // ---------------------------------------------------------------- helpers

    static String unique(String base) {
        return base + "." + RUN + "." + (++counter);
    }

    static void check(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static void eq(Object want, Object got, String what) {
        if (!Objects.equals(want, got)) throw new AssertionError(what + ": want <" + want + "> got <" + got + ">");
    }

    static org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory core() {
        return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(url);
    }

    static String accept(String key) throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] d = sha1.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII));
        return Base64.getEncoder().encodeToString(d);
    }

    /** A WebSocket frame. */
    record Frame(boolean fin, int opcode, byte[] payload) {
        String text() {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    /** A WebSocket client on a plain socket, to see every byte. */
    static final class RawWs implements AutoCloseable {
        final Socket socket;
        final DataInputStream in;
        final OutputStream out;
        String status;
        final Map<String, String> headers = new LinkedHashMap<>();
        final String key = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().substring(0, 16).getBytes());

        RawWs(String method, String extraHeaders) throws IOException {
            socket = new Socket(host, port);
            socket.setSoTimeout(10000);
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = socket.getOutputStream();
            String req = method + " / HTTP/1.1\r\nHost: " + host + ":" + port + "\r\n" + extraHeaders + "\r\n";
            out.write(req.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            status = line();
            for (String l = line(); !l.isEmpty(); l = line()) {
                int c = l.indexOf(':');
                headers.put(l.substring(0, c).trim().toLowerCase(Locale.ROOT), l.substring(c + 1).trim());
            }
        }

        static RawWs upgrade(String protocols, String version) throws IOException {
            String key = Base64.getEncoder().encodeToString(new byte[16]);
            String h = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\n"
                + (version == null ? "" : "Sec-WebSocket-Version: " + version + "\r\n")
                + (protocols == null ? "" : "Sec-WebSocket-Protocol: " + protocols + "\r\n");
            return new RawWs("GET", h);
        }

        static RawWs upgrade(String protocols) throws IOException {
            return upgrade(protocols, "13");
        }

        String line() throws IOException {
            StringBuilder b = new StringBuilder();
            int c;
            while ((c = in.read()) != -1 && c != '\n') {
                if (c != '\r') b.append((char) c);
            }
            if (c == -1 && b.length() == 0) throw new EOFException();
            return b.toString();
        }

        void send(int opcode, boolean fin, byte[] payload) throws IOException {
            ByteArrayOutputStream f = new ByteArrayOutputStream();
            f.write((fin ? 0x80 : 0) | opcode);
            if (payload.length < 126) {
                f.write(0x80 | payload.length);
            } else if (payload.length <= 0xFFFF) {
                f.write(0x80 | 126);
                f.write(payload.length >> 8);
                f.write(payload.length);
            } else {
                f.write(0x80 | 127);
                for (int i = 7; i >= 0; i--) f.write((int) ((long) payload.length >> (8 * i)));
            }
            byte[] mask = {7, 1, 9, 3};
            f.write(mask);
            for (int i = 0; i < payload.length; i++) f.write(payload[i] ^ mask[i % 4]);
            out.write(f.toByteArray());
            out.flush();
        }

        void sendBinary(byte[] payload) throws IOException {
            send(2, true, payload);
        }

        void sendText(String s) throws IOException {
            send(1, true, s.getBytes(StandardCharsets.UTF_8));
        }

        Frame read() throws IOException {
            int b0 = in.readUnsignedByte();
            int b1 = in.readUnsignedByte();
            check((b1 & 0x80) == 0, "server frames are not masked");
            long len = b1 & 0x7F;
            if (len == 126) len = in.readUnsignedShort();
            else if (len == 127) len = in.readLong();
            byte[] p = new byte[(int) len];
            in.readFully(p);
            return new Frame((b0 & 0x80) != 0, b0 & 0x0F, p);
        }

        /** The frames of the next message, from its first data frame to its last. */
        List<Frame> message() throws IOException {
            List<Frame> frames = new ArrayList<>();
            Frame f;
            do {
                f = read();
                frames.add(f);
            } while (!f.fin());
            return frames;
        }

        static byte[] join(List<Frame> frames) {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            for (Frame f : frames) b.writeBytes(f.payload());
            return b.toByteArray();
        }

        /** STOMP frames until one whose command is `command`. */
        String stompUntil(String command) throws IOException {
            StringBuilder pending = new StringBuilder();
            while (true) {
                pending.append(new String(join(message()), StandardCharsets.UTF_8));
                int at;
                while ((at = pending.indexOf("\0")) >= 0) {
                    String frame = pending.substring(0, at).replaceFirst("^\n+", "");
                    pending.delete(0, at + 1);
                    if (frame.startsWith(command + "\n")) return frame;
                    if (frame.startsWith("ERROR")) throw new AssertionError(frame);
                }
            }
        }

        boolean closedByServer() {
            try {
                return in.read() == -1;
            } catch (IOException e) {
                return true;
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    static String stompConnect(RawWs ws) throws IOException {
        ws.sendText("CONNECT\naccept-version:1.2\nhost:localhost\n\n\0");
        return ws.stompUntil("CONNECTED");
    }

    // ---------------------------------------------------------------- the handshake

    static void t_handshake() throws Exception {
        try (RawWs ws = RawWs.upgrade("foo, v12.stomp, v11.stomp")) {
            eq("HTTP/1.1 101 Switching Protocols", ws.status, "status");
            check("websocket".equalsIgnoreCase(ws.headers.get("upgrade")), "upgrade: " + ws.headers);
            check("upgrade".equalsIgnoreCase(ws.headers.get("connection")), "connection: " + ws.headers);
            eq(accept(Base64.getEncoder().encodeToString(new byte[16])), ws.headers.get("sec-websocket-accept"), "accept");
            eq("v12.stomp", ws.headers.get("sec-websocket-protocol"), "the client's first known subprotocol");
        }
    }

    static void t_everyProtocolsSubprotocol() throws Exception {
        for (String p : List.of("v10.stomp", "v11.stomp", "v12.stomp", "mqtt", "mqttv3.1", "amqp")) {
            try (RawWs ws = RawWs.upgrade(p)) {
                eq(p, ws.headers.get("sec-websocket-protocol"), "subprotocol " + p);
            }
        }
    }

    static void t_unknownSubprotocolStillUpgrades() throws Exception {
        try (RawWs ws = RawWs.upgrade("nothing-known")) {
            eq("HTTP/1.1 101 Switching Protocols", ws.status, "status");
            check(!ws.headers.containsKey("sec-websocket-protocol"), "chose " + ws.headers.get("sec-websocket-protocol"));
            eq("CONNECTED", stompConnect(ws).split("\n")[0], "STOMP inside");
        }
    }

    static void t_noSubprotocolStillUpgrades() throws Exception {
        try (RawWs ws = RawWs.upgrade(null)) {
            eq("HTTP/1.1 101 Switching Protocols", ws.status, "status");
            check(!ws.headers.containsKey("sec-websocket-protocol"), "chose a subprotocol");
        }
    }

    static void t_unsupportedVersion() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp", "12")) {
            check(ws.status.startsWith("HTTP/1.1 426"), "status " + ws.status);
            eq("13", ws.headers.get("sec-websocket-version"), "version offered");
        }
    }

    static void t_remotingUpgradeRefused() throws Exception {
        try (RawWs ws = new RawWs("GET", "Upgrade: activemq-remoting\r\nConnection: Upgrade\r\n")) {
            check(ws.status.startsWith("HTTP/1.1 403"), "status " + ws.status);
            check(ws.closedByServer(), "connection left open");
        }
    }

    // ---------------------------------------------------------------- framing

    static void t_pingAnswered() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            ws.send(9, true, "abc".getBytes());
            Frame f = ws.read();
            eq(10, f.opcode(), "pong");
            eq("abc", f.text(), "pong payload");
            eq("CONNECTED", stompConnect(ws).split("\n")[0], "STOMP after a ping");
        }
    }

    static void t_closeEchoed() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            stompConnect(ws);
            ByteArrayOutputStream p = new ByteArrayOutputStream();
            p.write(1000 >> 8);
            p.write(1000 & 0xFF);
            p.writeBytes("bye".getBytes());
            ws.send(8, true, p.toByteArray());
            Frame f;
            do {
                f = ws.read();
            } while (f.opcode() != 8);
            eq(1000, ((f.payload()[0] & 0xFF) << 8) | (f.payload()[1] & 0xFF), "close status");
            eq("bye", new String(f.payload(), 2, f.payload().length - 2), "close reason");
            check(ws.closedByServer(), "connection left open");
        }
    }

    static void t_fragmentedFramesJoin() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            byte[] connect = "CONNECT\naccept-version:1.2\nhost:localhost\n\n\0".getBytes();
            ws.send(2, false, Arrays.copyOfRange(connect, 0, 5));
            ws.send(0, false, Arrays.copyOfRange(connect, 5, 20));
            ws.send(0, true, Arrays.copyOfRange(connect, 20, connect.length));
            eq("CONNECTED", ws.stompUntil("CONNECTED").split("\n")[0], "connected");
        }
    }

    static void t_textAndBinaryFramesBothCarryData() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            String q = unique("wsmixed");
            ws.sendBinary("CONNECT\naccept-version:1.2\nhost:localhost\n\n\0".getBytes());
            ws.stompUntil("CONNECTED");
            ws.sendText("SUBSCRIBE\nid:1\ndestination:" + q + "\nsubscription-type:ANYCAST\nreceipt:r\n\n\0");
            ws.stompUntil("RECEIPT");
            ws.sendBinary(("SEND\ndestination:" + q + "\ndestination-type:ANYCAST\n\nhello\0").getBytes());
            String m = ws.stompUntil("MESSAGE");
            check(m.endsWith("\n\nhello"), "message " + m);
        }
    }

    static void t_serverFramesAreBinary() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            ws.sendText("CONNECT\naccept-version:1.2\nhost:localhost\n\n\0");
            List<Frame> frames = ws.message();
            eq(2, frames.get(0).opcode(), "first frame's opcode");
        }
    }

    static void t_largeMessageGoesOutInContinuationFrames() throws Exception {
        String q = unique("wslarge");
        String body = "x".repeat(5000);
        try (javax.jms.Connection c = core().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createQueue(q)).send(s.createTextMessage(body));
        }
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            stompConnect(ws);
            ws.sendText("SUBSCRIBE\nid:1\ndestination:" + q + "\ndestination-type:ANYCAST\n\n\0");
            List<Frame> frames = ws.message();
            check(frames.size() >= 3, "frames: " + frames.size());
            eq(2, frames.get(0).opcode(), "first opcode");
            for (int i = 0; i < frames.size(); i++) {
                Frame f = frames.get(i);
                check(f.payload().length <= MAX_PAYLOAD, "frame of " + f.payload().length);
                if (i > 0) eq(0, f.opcode(), "continuation opcode");
                eq(i == frames.size() - 1, f.fin(), "fin of frame " + i);
            }
            eq(MAX_PAYLOAD, frames.get(0).payload().length, "first frame full");
            String m = new String(RawWs.join(frames), StandardCharsets.UTF_8);
            check(m.startsWith("MESSAGE\n") && m.contains(body), "message reassembled");
        }
    }

    static void t_frameTooLargeClosesWith1009() throws Exception {
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            stompConnect(ws);
            ws.sendBinary(new byte[MAX_PAYLOAD + 1]);
            // Netty queues a close frame (1009) and closes the channel at
            // once: what arrives first varies.
            try {
                Frame f;
                do {
                    f = ws.read();
                } while (f.opcode() != 8);
                eq(1009, ((f.payload()[0] & 0xFF) << 8) | (f.payload()[1] & 0xFF), "close status");
                eq("Max frame length of " + MAX_PAYLOAD + " has been exceeded.", new String(f.payload(), 2, f.payload().length - 2), "reason");
            } catch (EOFException | SocketException closed) {
            }
            check(ws.closedByServer(), "connection left open");
        }
    }

    // ---------------------------------------------------------------- protocols

    static void t_stompWithJdkClient() throws Exception {
        String q = unique("wsjdk");
        BlockingQueue<String> got = new LinkedBlockingQueue<>();
        java.net.http.WebSocket.Listener listener = new java.net.http.WebSocket.Listener() {
            final ByteArrayOutputStream pending = new ByteArrayOutputStream();

            @Override
            public CompletionStage<?> onBinary(java.net.http.WebSocket ws, ByteBuffer data, boolean last) {
                byte[] b = new byte[data.remaining()];
                data.get(b);
                pending.writeBytes(b);
                if (last) {
                    got.add(pending.toString(StandardCharsets.UTF_8));
                    pending.reset();
                }
                ws.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onText(java.net.http.WebSocket ws, CharSequence data, boolean last) {
                got.add("TEXT:" + data);
                ws.request(1);
                return null;
            }
        };
        java.net.http.WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
            .subprotocols("v12.stomp")
            .buildAsync(URI.create("ws://" + host + ":" + port + "/"), listener).get(10, TimeUnit.SECONDS);
        try {
            eq("v12.stomp", ws.getSubprotocol(), "subprotocol");
            ws.sendText("CONNECT\naccept-version:1.2\nhost:localhost\n\n\0", true).get();
            String connected = got.poll(10, TimeUnit.SECONDS);
            check(connected != null && connected.startsWith("CONNECTED"), "connected: " + connected);
            ws.sendText("SUBSCRIBE\nid:1\ndestination:" + q + "\nsubscription-type:ANYCAST\nreceipt:r\n\n\0", true).get();
            check(got.poll(10, TimeUnit.SECONDS).startsWith("RECEIPT"), "receipt");
            ws.sendText("SEND\ndestination:" + q + "\ndestination-type:ANYCAST\n\nover the JDK's client\0", true).get();
            String m = got.poll(10, TimeUnit.SECONDS);
            check(m != null && m.startsWith("MESSAGE") && m.contains("\n\nover the JDK's client"), "message: " + m);
        } finally {
            ws.sendClose(1000, "done").get(10, TimeUnit.SECONDS);
        }
    }

    static void t_stompToCore() throws Exception {
        String q = unique("wstocore");
        try (RawWs ws = RawWs.upgrade("v12.stomp")) {
            stompConnect(ws);
            ws.sendText("SEND\ndestination:" + q + "\ndestination-type:ANYCAST\nreceipt:r\n\nfrom a browser\0");
            ws.stompUntil("RECEIPT");
        }
        try (javax.jms.Connection c = core().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            javax.jms.Message m = s.createConsumer(s.createQueue(q)).receive(5000);
            check(m != null, "no message");
            eq("from a browser", ((TextMessage) m).getText(), "body");
        }
    }

    static void t_mqttWithPaho() throws Exception {
        String topic = "ws/mqtt/" + unique("t").replace('.', '_');
        BlockingQueue<String> got = new LinkedBlockingQueue<>();
        MqttClient client = new MqttClient("ws://" + host + ":" + port, "ws-" + unique("c").replace('.', '-'), new MemoryPersistence());
        client.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) { }
            public void messageArrived(String t, MqttMessage m) { got.add(t + "=" + new String(m.getPayload())); }
            public void deliveryComplete(IMqttDeliveryToken token) { }
        });
        MqttConnectOptions o = new MqttConnectOptions();
        o.setCleanSession(true);
        client.connect(o);
        try {
            client.subscribe(topic, 1);
            client.publish(topic, "over WebSockets".getBytes(), 1, false);
            eq(topic + "=over WebSockets", got.poll(10, TimeUnit.SECONDS), "received");
        } finally {
            client.disconnect();
            client.close();
        }
    }

    static void t_mqttToCore() throws Exception {
        String topic = "wsmqtt/" + unique("t").replace('.', '_');
        try (javax.jms.Connection c = core().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createTopic(topic.replace('/', '.')));
            MqttClient client = new MqttClient("ws://" + host + ":" + port, "ws-" + unique("p").replace('.', '-'), new MemoryPersistence());
            client.connect();
            client.publish(topic, "mqtt over ws".getBytes(), 1, false);
            client.disconnect();
            client.close();
            javax.jms.Message m = consumer.receive(5000);
            check(m != null, "no message");
            eq("mqtt over ws", new String(m.getBody(byte[].class)), "body");
        }
    }

    static void t_amqpWithQpidJms() throws Exception {
        String q = unique("wsamqp");
        org.apache.qpid.jms.JmsConnectionFactory f = new org.apache.qpid.jms.JmsConnectionFactory("amqpws://" + host + ":" + port);
        try (javax.jms.Connection c = f.createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            javax.jms.Queue queue = s.createQueue(q);
            MessageConsumer consumer = s.createConsumer(queue);
            s.createProducer(queue).send(s.createTextMessage("amqp over ws"));
            javax.jms.Message m = consumer.receive(5000);
            check(m != null, "no message");
            eq("amqp over ws", ((TextMessage) m).getText(), "body");
        }
    }

    static void t_openWireHandshake() throws Exception {
        OpenWireFormat format = new OpenWireFormat(12);
        ByteSequence wfi = format.marshal(new WireFormatInfo() {{
            setVersion(12);
            setStackTraceEnabled(true);
            setTightEncodingEnabled(true);
        }});
        try (RawWs ws = RawWs.upgrade(null)) {
            ws.sendBinary(Arrays.copyOfRange(wfi.getData(), wfi.getOffset(), wfi.getOffset() + wfi.getLength()));
            byte[] reply = RawWs.join(ws.message());
            check(reply.length > 13, "reply of " + reply.length + " bytes");
            eq(1, (int) reply[4], "a WireFormatInfo");
            eq("ActiveMQ", new String(reply, 5, 8, StandardCharsets.US_ASCII), "magic");
        }
    }
}
