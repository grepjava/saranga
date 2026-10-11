// Phase 7 interop suite: STOMP 1.0, 1.1 and 1.2, ported from Artemis's
// StompTest, StompV11Test and StompV12Test. STOMP is spoken over a raw
// socket; JMS (Core) checks what crosses between the protocols.
//   java -cp '<artemis>/lib/*' Stomp.java tcp://host:port [test-name...]
// The broker runs tests/interop/stomp/broker.xml:
//   CONFIG=tests/interop/stomp/broker.xml SUITE_FILE=tests/interop/stomp/Stomp.java scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Stomp {
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
        for (java.lang.reflect.Method m : Stomp.class.getDeclaredMethods()) {
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

    // ---------------------------------------------------------------- client

    static class Frame {
        String command;
        List<String[]> headers = new ArrayList<>();
        byte[] body = new byte[0];

        String h(String name) {
            for (String[] kv : headers) if (kv[0].equals(name)) return kv[1];
            return null;
        }

        List<String> all(String name) {
            List<String> out = new ArrayList<>();
            for (String[] kv : headers) if (kv[0].equals(name)) out.add(kv[1]);
            return out;
        }

        String text() {
            return new String(body, StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder(command).append(headersText());
            if (body.length > 0) b.append(" body=").append(text());
            return b.toString();
        }

        String headersText() {
            StringBuilder b = new StringBuilder("{");
            for (String[] kv : headers) b.append(kv[0]).append('=').append(kv[1]).append(", ");
            return b.append('}').toString();
        }
    }

    static class Client implements Closeable {
        final Socket socket;
        final OutputStream out;
        final BlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        /** 1.1 and 1.2 frames escape their headers. */
        volatile boolean unescape;
        volatile int pings;
        volatile boolean eof;

        Client() throws IOException {
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            out = socket.getOutputStream();
            InputStream in = new BufferedInputStream(socket.getInputStream());
            Thread t = new Thread(() -> read(in));
            t.setDaemon(true);
            t.start();
        }

        void read(InputStream in) {
            try {
                boolean afterFrame = false;
                while (true) {
                    int b = in.read();
                    if (b < 0) break;
                    if (b == '\n') {
                        if (afterFrame) afterFrame = false; else pings++;
                        continue;
                    }
                    if (b == '\r') continue;
                    afterFrame = false;
                    ByteArrayOutputStream line = new ByteArrayOutputStream();
                    line.write(b);
                    Frame f = new Frame();
                    f.command = readLine(in, line);
                    String l;
                    int length = -1;
                    while (!(l = readLine(in, new ByteArrayOutputStream())).isEmpty()) {
                        int colon = l.indexOf(':');
                        String k = l.substring(0, colon), v = l.substring(colon + 1);
                        if (unescape) {
                            k = unescape(k);
                            v = unescape(v);
                        }
                        f.headers.add(new String[]{k, v});
                        if (k.equals("content-length") && length < 0) length = Integer.parseInt(v);
                    }
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    if (length >= 0) {
                        body.write(in.readNBytes(length));
                        if (in.read() != 0) throw new IOException("no NUL after content-length bytes");
                    } else {
                        int c;
                        while ((c = in.read()) > 0) body.write(c);
                        if (c < 0) break;
                    }
                    f.body = body.toByteArray();
                    frames.add(f);
                    afterFrame = true;
                }
            } catch (IOException e) {
                // closed
            }
            eof = true;
        }

        static String unescape(String s) {
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '\\' && i + 1 < s.length()) {
                    char n = s.charAt(++i);
                    b.append(n == 'n' ? '\n' : n == 'r' ? '\r' : n == 'c' ? ':' : n);
                } else {
                    b.append(c);
                }
            }
            return b.toString();
        }

        static String readLine(InputStream in, ByteArrayOutputStream line) throws IOException {
            int c;
            while ((c = in.read()) >= 0 && c != '\n') line.write(c);
            if (c < 0) throw new EOFException();
            byte[] b = line.toByteArray();
            int n = b.length;
            if (n > 0 && b[n - 1] == '\r') n--;
            return new String(b, 0, n, StandardCharsets.UTF_8);
        }

        void raw(String s) throws IOException {
            raw(s.getBytes(StandardCharsets.UTF_8));
        }

        void raw(byte[] b) throws IOException {
            out.write(b);
            out.flush();
        }

        void send(String command, String... headers) throws IOException {
            send(command, null, headers);
        }

        void send(String command, byte[] body, String... headers) throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            b.write((command + "\n").getBytes(StandardCharsets.UTF_8));
            for (String h : headers) b.write((h + "\n").getBytes(StandardCharsets.UTF_8));
            b.write('\n');
            if (body != null) b.write(body);
            b.write(0);
            raw(b.toByteArray());
        }

        Frame receive(long ms) throws InterruptedException {
            return frames.poll(ms, TimeUnit.MILLISECONDS);
        }

        Frame expect(String command) throws InterruptedException {
            Frame f = receive(5000);
            check(f != null, "no " + command + " frame");
            check(f.command.equals(command), "expected " + command + " but got " + f);
            return f;
        }

        void none(long ms) throws InterruptedException {
            Frame f = receive(ms);
            check(f == null, "unexpected frame " + f);
        }

        boolean closedWithin(long ms) throws InterruptedException {
            long end = System.currentTimeMillis() + ms;
            while (System.currentTimeMillis() < end) {
                if (eof) return true;
                Thread.sleep(20);
            }
            return eof;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /** A connected client: 1.0 without accept-version, else that version. */
    static Client connect(String version, String... extra) throws Exception {
        Client c = new Client();
        c.unescape = version != null && !version.equals("1.0");
        List<String> h = new ArrayList<>();
        h.add("login:guest");
        h.add("passcode:guest");
        if (version != null) {
            h.add("accept-version:" + version);
            h.add("host:localhost");
        }
        h.addAll(Arrays.asList(extra));
        c.send("CONNECT", h.toArray(new String[0]));
        Frame f = c.expect("CONNECTED");
        if (version != null && !version.contains(",")) check(version.equals(f.h("version")), "version " + f.h("version"));
        return c;
    }

    static void disconnect(Client c) throws Exception {
        c.send("DISCONNECT", "receipt:bye");
        c.expect("RECEIPT");
        check(c.closedWithin(5000), "the broker did not close the connection");
        c.close();
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void eq(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
    }

    static String name(String prefix) {
        return prefix + "." + RUN + "." + (counter++);
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------- JMS

    static Connection jms() throws Exception {
        Connection c = new ActiveMQConnectionFactory(url).createConnection();
        c.start();
        return c;
    }

    static Message receiveJms(String queue, long ms) throws Exception {
        try (Connection c = jms()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(queue));
            return mc.receive(ms);
        }
    }

    static boolean queueExists(String queue) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            return s.queueQuery(SimpleString.of(queue)).isExists();
        }
    }

    // ----------------------------------------------------------------- tests

    // CONNECT without accept-version: 1.0, a session ID, no version header.
    static void t_connect10() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "login:guest", "passcode:guest", "request-id:r1");
            Frame f = c.expect("CONNECTED");
            check(f.h("session") != null, "no session header");
            eq(null, f.h("version"), "version header");
            eq("r1", f.h("response-id"), "response-id");
            disconnect(c);
        }
    }

    static void t_connect11Negotiation() throws Exception {
        try (Client c = connect("1.0,1.1")) {
            check(c.receive(200) == null, "extra frame");
        }
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:1.0,1.1,1.2", "host:localhost");
            Frame f = c.expect("CONNECTED");
            eq("1.2", f.h("version"), "version");
            check(f.h("server") != null, "server header");
        }
    }

    // A 1.0 client that says so gets version:1.0 back.
    static void t_connectAccept10() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:1.0");
            Frame f = c.expect("CONNECTED");
            eq("1.0", f.h("version"), "version");
            eq(null, f.h("server"), "server header");
        }
    }

    static void t_unsupportedVersion() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:2.0,2.1", "host:localhost");
            Frame f = c.expect("ERROR");
            eq("1.0,1.1,1.2", f.h("version"), "version");
            eq("Supported protocol versions are v1.0 v1.1 v1.2", f.text(), "body");
            eq("AMQ339002 Stomp versions not supported 2.0,2.1", f.h("message"), "message");
            check(c.closedWithin(5000), "not closed");
        }
    }

    // Detected as STOMP by its first bytes, but not a CONNECT.
    static void t_notConnected() throws Exception {
        try (Client c = new Client()) {
            c.raw("CONNECTED\nversion:1.2\n\n\0");
            eq("AMQ339008 Connection has not been established.", c.expect("ERROR").h("message"), "message");
            c.send("CONNECT", "login:guest", "passcode:guest");
            c.expect("CONNECTED");
        }
    }

    // STOMP 1.1+: the STOMP command connects too.
    static void t_stompCommand() throws Exception {
        try (Client c = new Client()) {
            c.send("STOMP", "accept-version:1.2", "host:localhost", "login:guest", "passcode:guest");
            Frame f = c.expect("CONNECTED");
            eq("1.2", f.h("version"), "version");
            disconnect(c);
        }
    }

    // A client ending lines with \r\n speaks 1.2.
    static void t_crlfConnect() throws Exception {
        try (Client c = new Client()) {
            c.raw("CONNECT\r\naccept-version:1.2\r\nhost:localhost\r\n\r\n\0");
            Frame f = c.expect("CONNECTED");
            eq("1.2", f.h("version"), "version");
            String q = name("crlf");
            c.raw("SEND\r\ndestination:" + q + "\r\ndestination-type:ANYCAST\r\nreceipt:r\r\n\r\nhi\0");
            c.expect("RECEIPT");
            TextMessage m = (TextMessage) receiveJms(q, 5000);
            eq("hi", m.getText(), "body");
        }
    }

    static void t_disconnectReceipt() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("DISCONNECT", "receipt:77");
            Frame f = c.expect("RECEIPT");
            eq("77", f.h("receipt-id"), "receipt-id");
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_sendTextToJms() throws Exception {
        String q = name("sendText");
        try (Client c = connect(null)) {
            c.send("SEND", utf8("Hello World"), "destination:" + q, "destination-type:ANYCAST",
                   "correlation-id:c-17", "type:t-17", "priority:3", "persistent:true",
                   "foo:abc", "bar:123", "receipt:1");
            eq("1", c.expect("RECEIPT").h("receipt-id"), "receipt-id");
        }
        TextMessage m = (TextMessage) receiveJms(q, 5000);
        check(m != null, "nothing received");
        eq("Hello World", m.getText(), "body");
        eq("c-17", m.getJMSCorrelationID(), "correlation ID");
        eq("t-17", m.getJMSType(), "type");
        eq(3, m.getJMSPriority(), "priority");
        eq(DeliveryMode.PERSISTENT, m.getJMSDeliveryMode(), "delivery mode");
        eq("abc", m.getStringProperty("foo"), "foo");
        eq("123", m.getStringProperty("bar"), "bar");
        // Every other header becomes a property, receipt included.
        eq(q, m.getStringProperty("destination"), "destination property");
        eq("1", m.getStringProperty("receipt"), "receipt property");
    }

    static void t_sendBytesToJms() throws Exception {
        String q = name("sendBytes");
        byte[] data = {1, 2, 0, 3, (byte) 0xFF};
        try (Client c = connect("1.2")) {
            c.send("SEND", data, "destination:" + q, "destination-type:ANYCAST", "content-length:5", "receipt:r");
            c.expect("RECEIPT");
        }
        BytesMessage m = (BytesMessage) receiveJms(q, 5000);
        check(m != null, "nothing received");
        byte[] got = new byte[(int) m.getBodyLength()];
        m.readBytes(got);
        check(Arrays.equals(data, got), "body " + Arrays.toString(got));
        eq(DeliveryMode.NON_PERSISTENT, m.getJMSDeliveryMode(), "delivery mode");
    }

    static void t_jmsTextToStomp() throws Exception {
        String q = name("jmsText");
        try (Connection jc = jms(); Client c = connect("1.1")) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(q));
            TextMessage m = s.createTextMessage("Hello from JMS");
            m.setJMSCorrelationID("corr");
            m.setJMSType("typ");
            m.setStringProperty("color", "blue");
            m.setIntProperty("count", 7);
            m.setBooleanProperty("flag", true);
            m.setDoubleProperty("d", 1.5);
            p.send(m, DeliveryMode.PERSISTENT, 6, 0);
            c.send("SUBSCRIBE", "destination:" + q, "id:s1", "ack:auto");
            Frame f = c.expect("MESSAGE");
            eq("Hello from JMS", f.text(), "body");
            eq(q, f.h("destination"), "destination");
            eq("s1", f.h("subscription"), "subscription");
            eq("corr", f.h("correlation-id"), "correlation-id");
            eq("typ", f.h("type"), "type");
            eq("6", f.h("priority"), "priority");
            eq("true", f.h("persistent"), "persistent");
            eq("false", f.h("redelivered"), "redelivered");
            eq("0", f.h("expires"), "expires");
            eq("blue", f.h("color"), "color");
            eq("7", f.h("count"), "count");
            eq("true", f.h("flag"), "flag");
            eq("1.5", f.h("d"), "d");
            eq("ANYCAST", f.h("destination-type"), "destination-type");
            check(f.h("message-id").matches("\\d+,\\d+"), "message-id " + f.h("message-id"));
            check(Long.parseLong(f.h("timestamp")) > 0, "timestamp");
            // 1.1 and 1.2 frames always carry the length of a body.
            eq("14", f.h("content-length"), "content-length of text");
        }
    }

    static void t_jmsBytesToStomp() throws Exception {
        String q = name("jmsBytes");
        byte[] data = {0, 1, 2, 3, 0, 5};
        try (Connection jc = jms(); Client c = connect("1.2")) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(data);
            s.createProducer(s.createQueue(q)).send(m);
            c.send("SUBSCRIBE", "destination:" + q, "id:s1");
            Frame f = c.expect("MESSAGE");
            eq("6", f.h("content-length"), "content-length");
            check(Arrays.equals(data, f.body), "body " + Arrays.toString(f.body));
            eq(null, f.h("ack"), "ack header in auto mode");
        }
    }

    static void t_stompToStompProperties() throws Exception {
        String q = name("props");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + q, "id:sub", "destination-type:ANYCAST", "subscription-type:ANYCAST", "receipt:s");
            c.expect("RECEIPT");
            c.send("SEND", utf8("x"), "destination:" + q, "destination-type:ANYCAST", "reply-to:/queue/r", "JMSXGroupID:g1", "content-type:text/plain", "expires:0");
            Frame f = c.expect("MESSAGE");
            eq("/queue/r", f.h("reply-to"), "reply-to");
            eq("text/plain", f.h("content-type"), "content-type");
            eq("g1", f.h("_AMQ_GROUP_ID"), "group ID");
            eq("x", f.text(), "body");
        }
    }

    static void t_jmsxGroupId() throws Exception {
        String q = name("group");
        try (Client c = connect(null)) {
            c.send("SEND", utf8("g"), "destination:" + q, "destination-type:ANYCAST", "JMSXGroupID:TEST", "receipt:r");
            c.expect("RECEIPT");
        }
        Message m = receiveJms(q, 5000);
        eq("TEST", m.getStringProperty("JMSXGroupID"), "JMSXGroupID");
    }

    static void t_receiptBeforeMessages() throws Exception {
        String q = name("receiptFirst");
        try (Connection jc = jms(); Client c = connect("1.2")) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(q));
            for (int i = 0; i < 5; i++) p.send(s.createTextMessage("m" + i));
            c.send("SUBSCRIBE", "destination:" + q, "id:s1", "receipt:sub");
            eq("sub", c.expect("RECEIPT").h("receipt-id"), "receipt-id");
            for (int i = 0; i < 5; i++) eq("m" + i, c.expect("MESSAGE").text(), "message " + i);
        }
    }

    static void t_messagesInOrder() throws Exception {
        String q = name("order");
        try (Client c = connect("1.0")) {
            for (int i = 0; i < 50; i++) c.send("SEND", utf8("n" + i), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "ack:auto");
            for (int i = 0; i < 50; i++) eq("n" + i, c.expect("MESSAGE").text(), "message " + i);
        }
    }

    static void t_autoAckRemovesMessages() throws Exception {
        String q = name("autoAck");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("a"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:auto");
            c.expect("MESSAGE");
            disconnect(c);
        }
        check(receiveJms(q, 500) == null, "message left after auto ack");
    }

    static void t_clientAckCumulative() throws Exception {
        String q = name("clientAck");
        try (Client c = connect("1.0")) {
            for (int i = 0; i < 3; i++) c.send("SEND", utf8("c" + i), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "ack:client");
            c.expect("MESSAGE");
            Frame second = c.expect("MESSAGE");
            c.expect("MESSAGE");
            // Acknowledging the second acknowledges the first too.
            c.send("ACK", "message-id:" + second.h("message-id"), "receipt:a");
            c.expect("RECEIPT");
            disconnect(c);
        }
        TextMessage m = (TextMessage) receiveJms(q, 5000);
        eq("c2", m.getText(), "the unacknowledged message");
        check(receiveJms(q, 300) == null, "more than one left");
    }

    static void t_clientAckRedelivery() throws Exception {
        String q = name("redeliver");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("r"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client");
            eq("false", c.expect("MESSAGE").h("redelivered"), "first delivery");
            disconnect(c);
        }
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client");
            Frame f = c.expect("MESSAGE");
            eq("true", f.h("redelivered"), "second delivery");
            c.send("ACK", "message-id:" + f.h("message-id"), "subscription:s", "receipt:a");
            c.expect("RECEIPT");
        }
    }

    static void t_clientIndividual() throws Exception {
        String q = name("individual");
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8("1"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SEND", utf8("2"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client-individual");
            Frame one = c.expect("MESSAGE");
            c.expect("MESSAGE");
            eq(one.h("message-id"), one.h("ack"), "ack header");
            c.send("ACK", "id:" + one.h("ack"), "receipt:a");
            c.expect("RECEIPT");
            disconnect(c);
        }
        TextMessage m = (TextMessage) receiveJms(q, 5000);
        eq("2", m.getText(), "the unacknowledged message");
    }

    static void t_nack11Discards() throws Exception {
        String q = name("nack");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("n"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client");
            Frame f = c.expect("MESSAGE");
            c.send("NACK", "message-id:" + f.h("message-id"), "subscription:s", "receipt:n");
            c.expect("RECEIPT");
            disconnect(c);
        }
        check(receiveJms(q, 500) == null, "NACK left the message");
    }

    static void t_nack10IsUnknown() throws Exception {
        try (Client c = connect(null)) {
            c.send("NACK", "message-id:1,1");
            Frame f = c.expect("ERROR");
            check(f.h("message").startsWith("AMQ339023 Invalid STOMP frame"), f.toString());
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_ack12NeedsId() throws Exception {
        try (Client c = connect("1.2")) {
            c.send("ACK", "message-id:1,1");
            Frame f = c.expect("ERROR");
            eq("AMQ339039 No id header in ACK/NACK frame.", f.h("message"), "message");
            // 1.2 closes the connection after an ERROR.
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_ack11NeedsSubscription() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("ACK", "message-id:1,1", "receipt:r");
            Frame f = c.expect("ERROR");
            eq("AMQ339037 Must specify the subscription's id", f.h("message"), "message");
            eq("r", f.h("receipt-id"), "receipt-id on the error");
            // 1.1 keeps the connection.
            c.send("DISCONNECT", "receipt:d");
            c.expect("RECEIPT");
        }
    }

    static void t_ackWrongSubscription() throws Exception {
        String q = name("wrongSub");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("w"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:right", "ack:client");
            Frame f = c.expect("MESSAGE");
            c.send("ACK", "message-id:" + f.h("message-id"), "subscription:wrong");
            Frame e = c.expect("ERROR");
            eq("AMQ339026 subscription id wrong does not match right", e.h("message"), "message");
        }
    }

    static void t_ackUnknownMessage() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + name("unknownAck"), "destination-type:ANYCAST", "subscription-type:ANYCAST", "id:s", "ack:client");
            c.send("ACK", "message-id:12,34", "subscription:s");
            Frame e = c.expect("ERROR");
            eq("AMQ339025 failed to ack because no message with id 12,34", e.h("message"), "message");
        }
    }

    static void t_transactionCommit() throws Exception {
        String q = name("txCommit");
        try (Client c = connect("1.0")) {
            c.send("BEGIN", "transaction:tx1");
            c.send("SEND", utf8("t1"), "destination:" + q, "destination-type:ANYCAST", "transaction:tx1");
            c.send("SEND", utf8("t2"), "destination:" + q, "destination-type:ANYCAST", "transaction:tx1", "receipt:s");
            c.expect("RECEIPT");
            check(receiveJms(q, 300) == null, "visible before commit");
            c.send("COMMIT", "transaction:tx1", "receipt:c");
            c.expect("RECEIPT");
        }
        eq("t1", ((TextMessage) receiveJms(q, 5000)).getText(), "first");
        eq("t2", ((TextMessage) receiveJms(q, 5000)).getText(), "second");
    }

    static void t_transactionAbort() throws Exception {
        String q = name("txAbort");
        try (Client c = connect("1.1")) {
            c.send("BEGIN", "transaction:tx1");
            c.send("SEND", utf8("gone"), "destination:" + q, "destination-type:ANYCAST", "transaction:tx1");
            c.send("ABORT", "transaction:tx1", "receipt:a");
            c.expect("RECEIPT");
            c.send("SEND", utf8("kept"), "destination:" + q, "destination-type:ANYCAST", "receipt:s");
            c.expect("RECEIPT");
        }
        eq("kept", ((TextMessage) receiveJms(q, 5000)).getText(), "after abort");
        check(receiveJms(q, 300) == null, "aborted message delivered");
    }

    static void t_successiveTransactionsSameId() throws Exception {
        String q = name("txAgain");
        try (Client c = connect("1.1")) {
            for (int i = 0; i < 2; i++) {
                c.send("BEGIN", "transaction:tx1");
                c.send("SEND", utf8("s" + i), "destination:" + q, "destination-type:ANYCAST", "transaction:tx1");
                c.send("COMMIT", "transaction:tx1", "receipt:c" + i);
                c.expect("RECEIPT");
            }
        }
        eq("s0", ((TextMessage) receiveJms(q, 5000)).getText(), "first");
        eq("s1", ((TextMessage) receiveJms(q, 5000)).getText(), "second");
    }

    static void t_beginTwice() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("BEGIN", "transaction:tx1");
            c.send("BEGIN", "transaction:tx1");
            Frame e = c.expect("ERROR");
            eq("Transaction already started: tx1", e.h("message"), "message");
        }
    }

    static void t_commitWithoutBegin() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("COMMIT", "transaction:tx9");
            eq("AMQ339013 Error committing tx9", c.expect("ERROR").h("message"), "message");
            c.send("ABORT", "transaction:tx9");
            eq("No transaction started: tx9", c.expect("ERROR").h("message"), "message");
            c.send("COMMIT");
            eq("AMQ339030 transaction header is mandatory to COMMIT a transaction", c.expect("ERROR").h("message"), "message");
            c.send("BEGIN");
            eq("AMQ339032 Need a transaction id to begin", c.expect("ERROR").h("message"), "message");
        }
    }

    // Acks are never part of a transaction.
    static void t_clientAckNotPartOfTransaction() throws Exception {
        String q = name("ackNoTx");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("a"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client");
            Frame f = c.expect("MESSAGE");
            c.send("BEGIN", "transaction:tx1");
            c.send("ACK", "message-id:" + f.h("message-id"), "subscription:s", "transaction:tx1");
            c.send("ABORT", "transaction:tx1", "receipt:a");
            c.expect("RECEIPT");
            disconnect(c);
        }
        check(receiveJms(q, 500) == null, "the ACK was rolled back");
    }

    static void t_topicSubscribe() throws Exception {
        try (Connection jc = jms(); Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:t", "receipt:s");
            c.expect("RECEIPT");
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic("testtopic")).send(s.createTextMessage("to topic"));
            Frame f = c.expect("MESSAGE");
            eq("to topic", f.text(), "body");
            eq("testtopic", f.h("destination"), "destination");
            c.send("UNSUBSCRIBE", "id:t", "receipt:u");
            c.expect("RECEIPT");
            s.createProducer(s.createTopic("testtopic")).send(s.createTextMessage("after"));
            c.none(500);
        }
    }

    static void t_twoSubscriptionsDifferentMessageIds() throws Exception {
        try (Connection jc = jms(); Client c = connect("1.2")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:a", "ack:client");
            c.send("SUBSCRIBE", "destination:testtopic", "id:b", "ack:client", "receipt:s");
            c.expect("RECEIPT");
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic("testtopic")).send(s.createTextMessage("x"));
            Frame f1 = c.expect("MESSAGE"), f2 = c.expect("MESSAGE");
            check(!f1.h("ack").equals(f2.h("ack")), "same ack ID " + f1.h("ack"));
            check(!f1.h("subscription").equals(f2.h("subscription")), "same subscription");
        }
    }

    static void t_durableSubscriber() throws Exception {
        String sub = name("durable");
        String queue = "myclient." + sub;
        try (Client c = connect("1.1", "client-id:myclient")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:d", "durable-subscription-name:" + sub, "receipt:s");
            c.expect("RECEIPT");
            disconnect(c);
        }
        check(queueExists(queue), "durable queue " + queue + " missing");
        try (Connection jc = jms()) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic("testtopic")).send(s.createTextMessage("while away"));
        }
        try (Client c = connect("1.1", "client-id:myclient")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:d", "durable-subscription-name:" + sub);
            eq("while away", c.expect("MESSAGE").text(), "body");
            c.send("UNSUBSCRIBE", "id:d", "durable-subscription-name:" + sub, "receipt:u");
            c.expect("RECEIPT");
        }
        check(!queueExists(queue), "durable queue still there");
    }

    static void t_durableNeedsClientId() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:d", "durable-subscription-name:x");
            Frame f = c.expect("ERROR");
            check(f.h("message").startsWith("AMQ339016 Error creating subscription"), f.toString());
        }
    }

    static void t_noLocal() throws Exception {
        try (Connection jc = jms(); Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:n", "no-local:true", "receipt:s");
            c.expect("RECEIPT");
            c.send("SEND", utf8("mine"), "destination:testtopic", "receipt:r");
            c.expect("RECEIPT");
            c.none(500);
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic("testtopic")).send(s.createTextMessage("theirs"));
            eq("theirs", c.expect("MESSAGE").text(), "body");
        }
    }

    static void t_selector() throws Exception {
        String q = name("selector");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("red"), "destination:" + q, "destination-type:ANYCAST", "color:red");
            c.send("SEND", utf8("blue"), "destination:" + q, "destination-type:ANYCAST", "color:blue");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "selector:color = 'blue'");
            eq("blue", c.expect("MESSAGE").text(), "body");
            c.none(500);
        }
    }

    static void t_emptySelector() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:s", "selector:");
            eq("Selector cannot be empty", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_invalidSelector() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testQueue", "id:s", "selector:foo is bar");
            Frame f = c.expect("ERROR");
            eq("Invalid selector \"foo is bar\"", f.h("message"), "message");
            eq("text/plain", f.h("content-type"), "content-type");
        }
    }

    static void t_sendNoAutoCreate() throws Exception {
        String q = name("noauto");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("x"), "destination:" + q, "receipt:r");
            Frame f = c.expect("ERROR");
            eq("AMQ339001 Destination does not exist " + q, f.h("message"), "message");
            eq("r", f.h("receipt-id"), "receipt-id");
        }
    }

    static void t_subscribeNoAutoCreate() throws Exception {
        String q = name("noauto");
        try (Client c = connect("1.0")) {
            c.send("SUBSCRIBE", "destination:" + q, "receipt:r");
            Frame f = c.expect("ERROR");
            eq("AMQ339001 Destination does not exist " + q, f.h("message"), "message");
            // A failed SUBSCRIBE's error has no receipt-id.
            eq(null, f.h("receipt-id"), "receipt-id");
        }
    }

    // Without a destination-type, a new address gets the default routing
    // type (multicast), so a later anycast consumer misses the message.
    static void t_defaultRoutingMulticast() throws Exception {
        String a = name("deflt");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + a, "id:s", "receipt:s");
            c.expect("RECEIPT");
            c.send("SEND", utf8("m"), "destination:" + a);
            Frame f = c.expect("MESSAGE");
            eq(null, f.h("destination-type"), "destination-type");
        }
        check(!queueExists(a), "an anycast queue named after the address");
    }

    static void t_anycastDestinationType() throws Exception {
        String q = name("anyType");
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8("m"), "destination:" + q, "destination-type:ANYCAST", "receipt:r");
            c.expect("RECEIPT");
            check(queueExists(q), "anycast queue not auto-created");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "subscription-type:ANYCAST");
            eq("ANYCAST", c.expect("MESSAGE").h("destination-type"), "destination-type");
        }
    }

    static void t_badDestinationType() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("m"), "destination:x", "destination-type:SIDEWAYS");
            eq("AMQ339031 Error handling send", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_slashAnycastPrefix() throws Exception {
        String q = name("slashAny");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:/queue/" + q, "id:s", "receipt:s");
            c.expect("RECEIPT");
            check(queueExists(q), "the prefix did not make an anycast queue");
            c.send("SEND", utf8("p"), "destination:/queue/" + q);
            Frame f = c.expect("MESSAGE");
            // The prefix comes back on the destination.
            eq("/queue/" + q, f.h("destination"), "destination");
            eq("ANYCAST", f.h("destination-type"), "destination-type");
        }
        try (Connection jc = jms(); Client c = connect("1.1")) {
            c.send("SEND", utf8("p2"), "destination:/queue/" + q, "receipt:r");
            c.expect("RECEIPT");
            eq("p2", ((TextMessage) receiveJms(q, 5000)).getText(), "JMS receive");
        }
    }

    static void t_slashMulticastPrefix() throws Exception {
        String t = name("slashMulti");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:/topic/" + t, "id:s", "receipt:s");
            c.expect("RECEIPT");
            c.send("SEND", utf8("p"), "destination:/topic/" + t);
            Frame f = c.expect("MESSAGE");
            eq("/topic/" + t, f.h("destination"), "destination");
            eq("MULTICAST", f.h("destination-type"), "destination-type");
        }
    }

    static void t_scheduledDelay() throws Exception {
        String q = name("delay");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "subscription-type:ANYCAST", "receipt:s");
            c.expect("RECEIPT");
            long start = System.currentTimeMillis();
            c.send("SEND", utf8("later"), "destination:" + q, "destination-type:ANYCAST", "AMQ_SCHEDULED_DELAY:1500");
            Frame f = c.receive(5000);
            check(f != null, "never delivered");
            check(System.currentTimeMillis() - start >= 1300, "delivered too early");
        }
    }

    static void t_scheduledBadValue() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("x"), "destination:testQueue", "AMQ_SCHEDULED_DELAY:soon");
            eq("AMQ339031 Error handling send", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_expiredNotDelivered() throws Exception {
        String q = name("expired");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("old"), "destination:" + q, "destination-type:ANYCAST", "expires:" + (System.currentTimeMillis() - 10000), "receipt:r");
            c.expect("RECEIPT");
        }
        check(receiveJms(q, 500) == null, "expired message delivered");
    }

    static void t_utf8Body() throws Exception {
        String q = name("utf8");
        String text = "AêñüC 中文 😀";
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8(text), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s");
            eq(text, c.expect("MESSAGE").text(), "body");
        }
    }

    static void t_headerEscaping() throws Exception {
        String q = name("escape");
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8("e"), "destination:" + q, "destination-type:ANYCAST", "a\\cb:x\\ny\\\\z\\r");
            c.send("SUBSCRIBE", "destination:" + q, "id:s");
            Frame f = c.expect("MESSAGE");
            // The broker escapes them again; the client unescapes.
            eq("x\ny\\z\r", f.h("a:b"), "escaped header");
        }
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8("e"), "destination:" + q, "destination-type:ANYCAST", "k:v", "receipt:r");
            c.expect("RECEIPT");
        }
        Message m = receiveJms(q, 5000);
        check(m != null, "nothing");
    }

    static void t_headerValueColonsDropped11() throws Exception {
        String q = name("colons");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("c"), "destination:" + q, "destination-type:ANYCAST", "url:http://x", "receipt:r");
            c.expect("RECEIPT");
        }
        // Artemis drops an unescaped ':' after the first in a 1.1 header.
        eq("http//x", receiveJms(q, 5000).getStringProperty("url"), "url");
    }

    static void t_headerTrimmed10() throws Exception {
        String q = name("trim");
        try (Client c = connect(null)) {
            c.send("SEND", utf8("t"), "destination: " + q, "destination-type:ANYCAST", "pad:   v ", "receipt:r");
            c.expect("RECEIPT");
        }
        eq("v ", receiveJms(q, 5000).getStringProperty("pad"), "pad");
    }

    static void t_repeatedHeaders() throws Exception {
        String q = name("repeat");
        try (Client c = connect("1.2")) {
            c.send("SEND", utf8("r"), "destination:" + q, "destination-type:ANYCAST", "foo:first", "foo:second", "receipt:r");
            c.expect("RECEIPT");
        }
        eq("first", receiveJms(q, 5000).getStringProperty("foo"), "1.2 keeps the first");
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8("r"), "destination:" + q, "destination-type:ANYCAST", "foo:first", "foo:second", "receipt:r");
            c.expect("RECEIPT");
        }
        eq("second", receiveJms(q, 5000).getStringProperty("foo"), "1.1 keeps the last");
    }

    static void t_undefinedEscape() throws Exception {
        try (Client c = connect("1.1")) {
            c.raw("SEND\ndestination:testQueue\nbad:\\t\n\nx\0");
            Frame f = c.expect("ERROR");
            eq("AMQ339040 Undefined escape sequence \\t", f.h("message"), "message");
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_invalidCommand() throws Exception {
        try (Client c = connect("1.2")) {
            c.raw("FOO\nk:v\n\n\0");
            Frame f = c.expect("ERROR");
            check(f.h("message").startsWith("AMQ339023 Invalid STOMP frame F,O,O,10,"), f.toString());
            check(f.text().startsWith("AMQ339024: Invalid STOMP frame: F,O,O,10,"), f.text());
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_unknownCommand() throws Exception {
        try (Client c = connect("1.1")) {
            c.raw("MESSAGE\n\n\0");
            eq("AMQ339029 Unsupported command MESSAGE", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_leadingNewLines() throws Exception {
        String q = name("leading");
        try (Client c = connect("1.1")) {
            c.raw("\n\n\nSEND\ndestination:" + q + "\ndestination-type:ANYCAST\nreceipt:r\n\nhi\0\n\n");
            c.expect("RECEIPT");
        }
        eq("hi", ((TextMessage) receiveJms(q, 5000)).getText(), "body");
    }

    static void t_heartBeatServerPings() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:1.1", "host:localhost", "heart-beat:0,500");
            Frame f = c.expect("CONNECTED");
            eq("500,30000", f.h("heart-beat"), "heart-beat");
            Thread.sleep(1700);
            check(c.pings >= 2, "pings: " + c.pings);
        }
    }

    static void t_heartBeatClientTtl() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:1.2", "host:localhost", "heart-beat:500,0");
            Frame f = c.expect("CONNECTED");
            eq("0,500", f.h("heart-beat"), "heart-beat");
            // The TTL is twice the client's ping period: silence closes it.
            Frame e = c.receive(5000);
            check(e != null && e.command.equals("ERROR"), "no ERROR: " + e);
            check(e.h("message").startsWith("AMQ229014"), e.toString());
            check(c.closedWithin(5000), "not closed");
        }
    }

    static void t_heartBeatKeepsAlive() throws Exception {
        try (Client c = new Client()) {
            c.send("CONNECT", "accept-version:1.2", "host:localhost", "heart-beat:500,0");
            c.expect("CONNECTED");
            for (int i = 0; i < 8; i++) {
                Thread.sleep(300);
                c.raw("\n");
            }
            check(!c.eof, "closed although pinging");
            c.none(100);
        }
    }

    static void t_heartBeatBadHeader() throws Exception {
        try (Client c = new Client()) {
            c.unescape = true;
            c.send("CONNECT", "accept-version:1.1", "host:localhost", "heart-beat:1000");
            eq("Incorrect heartbeat header 1000", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_consumerWindowZero() throws Exception {
        String q = name("window0");
        try (Client c = connect("1.2")) {
            for (int i = 0; i < 3; i++) c.send("SEND", utf8("w" + i), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client-individual", "consumer-window-size:0");
            Frame f = c.expect("MESSAGE");
            c.none(500);
            c.send("ACK", "id:" + f.h("ack"));
            eq("w1", c.expect("MESSAGE").text(), "after the ack");
        }
    }

    static void t_consumerWindowBytes() throws Exception {
        String q = name("windowN");
        try (Client c = connect("1.2")) {
            for (int i = 0; i < 4; i++) c.send("SEND", utf8("w" + i), "destination:" + q, "destination-type:ANYCAST");
            // One credit's worth of bytes lets one message through, and the
            // window runs out until the client acknowledges.
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "ack:client", "activemq.prefetchSize:1");
            Frame f = c.expect("MESSAGE");
            c.none(500);
            c.send("ACK", "id:" + f.h("ack"));
            c.expect("MESSAGE");
        }
    }

    static void t_errorWithReceipt() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("UNSUBSCRIBE", "id:nope", "receipt:u");
            Frame f = c.expect("ERROR");
            eq("Cannot unsubscribe as no subscription exists for id: nope", f.h("message"), "message");
            eq("u", f.h("receipt-id"), "receipt-id");
        }
    }

    static void t_unsubscribeNeedsId() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("UNSUBSCRIBE", "destination:testQueue");
            eq("AMQ339037 Must specify the subscription's id", c.expect("ERROR").h("message"), "message");
        }
        try (Client c = connect(null)) {
            c.send("UNSUBSCRIBE");
            eq("AMQ339035 Must specify the subscription's id or the destination you are unsubscribing from", c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_unsubscribe10ByDestination() throws Exception {
        String q = name("unsub10");
        try (Client c = connect(null)) {
            c.send("SUBSCRIBE", "destination:" + q, "destination-type:ANYCAST", "subscription-type:ANYCAST", "receipt:s");
            c.expect("RECEIPT");
            c.send("UNSUBSCRIBE", "destination:" + q, "receipt:u");
            c.expect("RECEIPT");
            c.send("SEND", utf8("x"), "destination:" + q, "destination-type:ANYCAST");
            c.none(500);
        }
    }

    static void t_duplicateSubscriptionId() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:dup", "receipt:a");
            c.expect("RECEIPT");
            c.send("SUBSCRIBE", "destination:testtopic", "id:dup");
            eq("There already is a subscription for: dup. Either use unique subscription IDs or do not create multiple subscriptions for the same destination",
               c.expect("ERROR").h("message"), "message");
        }
    }

    static void t_largeMessages() throws Exception {
        String q = name("large");
        byte[] data = new byte[300 * 1024];
        new Random(7).nextBytes(data);
        try (Client c = connect("1.2")) {
            c.send("SEND", data, "destination:" + q, "destination-type:ANYCAST", "content-length:" + data.length, "receipt:r");
            c.expect("RECEIPT");
        }
        try (Connection jc = jms()) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            BytesMessage m = (BytesMessage) s.createConsumer(s.createQueue(q)).receive(5000);
            check(m != null, "nothing received");
            byte[] got = new byte[(int) m.getBodyLength()];
            m.readBytes(got);
            check(Arrays.equals(data, got), "JMS got " + got.length + " bytes");
        }
        // And back out to STOMP from a JMS large message.
        try (Connection jc = jms(); Client c = connect("1.2")) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            BytesMessage b = s.createBytesMessage();
            b.writeBytes(data);
            s.createProducer(s.createQueue(q)).send(b);
            c.send("SUBSCRIBE", "destination:" + q, "id:s");
            Frame f = c.expect("MESSAGE");
            check(Arrays.equals(data, f.body), "STOMP got " + f.body.length + " bytes");
        }
    }

    static void t_largeText() throws Exception {
        String q = name("largeText");
        String text = "x".repeat(250 * 1024);
        try (Client c = connect("1.1")) {
            c.send("SEND", utf8(text), "destination:" + q, "destination-type:ANYCAST", "receipt:r");
            c.expect("RECEIPT");
            c.send("SUBSCRIBE", "destination:" + q, "id:s");
            eq(text.length(), c.expect("MESSAGE").text().length(), "length");
        }
    }

    static void t_tempQueueRemovedOnDisconnect() throws Exception {
        String t = name("tempGone");
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:" + t, "id:s", "subscription-type:MULTICAST", "receipt:s");
            c.expect("RECEIPT");
            try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
                 ClientSessionFactory sf = locator.createSessionFactory();
                 ClientSession s = sf.createSession()) {
                eq(1, s.addressQuery(SimpleString.of(t)).getQueueNames().size(), "queues while subscribed");
            }
            disconnect(c);
        }
        Thread.sleep(300);
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            eq(0, s.addressQuery(SimpleString.of(t)).getQueueNames().size(), "queues after disconnect");
        }
    }

    static void t_transactedSendsNotNoLocal() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("SUBSCRIBE", "destination:testtopic", "id:n", "no-local:true", "receipt:s");
            c.expect("RECEIPT");
            c.send("BEGIN", "transaction:t");
            c.send("SEND", utf8("tx"), "destination:testtopic", "transaction:t");
            c.send("COMMIT", "transaction:t");
            // A transacted session has no no-local subscriptions, so the
            // connection ID is not stamped and its own send comes back.
            eq("tx", c.expect("MESSAGE").text(), "body");
        }
    }

    static void t_messageIdHeaderFromConsumer() throws Exception {
        String q = name("mid");
        try (Client c = connect("1.0")) {
            c.send("SEND", utf8("m"), "destination:" + q, "destination-type:ANYCAST");
            c.send("SUBSCRIBE", "destination:" + q, "ack:client", "id:s");
            Frame f = c.expect("MESSAGE");
            String[] parts = f.h("message-id").split(",");
            eq(2, parts.length, "message-id parts");
            c.send("ACK", "message-id:" + f.h("message-id"), "receipt:a");
            c.expect("RECEIPT");
        }
    }

    static void t_secondConnect() throws Exception {
        try (Client c = connect("1.1")) {
            c.send("CONNECT", "accept-version:1.1", "host:localhost");
            Frame f = c.expect("CONNECTED");
            eq("1.1", f.h("version"), "version");
        }
    }

    static void t_jmsToStompSubscriberThenJmsSees() throws Exception {
        String q = name("multi");
        try (Connection jc = jms(); Client c = connect("1.1")) {
            Session s = jc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(q));
            c.send("SUBSCRIBE", "destination:" + q, "id:s", "subscription-type:ANYCAST", "receipt:s");
            c.expect("RECEIPT");
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            for (int i = 0; i < 10; i++) p.send(s.createTextMessage("m" + i));
            int stomp = 0, jms = 0;
            long end = System.currentTimeMillis() + 5000;
            while (stomp + jms < 10 && System.currentTimeMillis() < end) {
                if (c.receive(50) != null) stomp++;
                if (mc.receive(50) != null) jms++;
            }
            eq(10, stomp + jms, "messages received");
            check(stomp > 0 && jms > 0, "stomp " + stomp + ", jms " + jms);
        }
    }
}
