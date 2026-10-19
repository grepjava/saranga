// Phase 7 interop suite: OpenWire with the ActiveMQ 5.19.10 client that
// Artemis ships, ported mostly from Artemis's SimpleOpenWireTest and its
// neighbours (AdvisoryOpenWireTest, VirtualTopicToFQQNOpenWireTest,
// CompositeDestinationTest, OpenWireScheduledDelayTest, JMSPullConsumerTest).
// Core (Artemis JMS) checks what crosses between the protocols.
//   java -cp '<artemis>/lib/*' OpenWire.java tcp://host:port [test-name...]
// The broker runs tests/interop/openwire/broker.xml:
//   CONFIG=tests/interop/openwire/broker.xml SUITE_FILE=tests/interop/openwire/OpenWire.java scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;
import javax.jms.Queue;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DataStructure;

public class OpenWire {
    static String url;
    static String host;
    static int port;
    static String ow;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int counter;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        URI u = new URI(url);
        host = u.getHost();
        port = u.getPort();
        ow = "tcp://" + host + ":" + port;
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : OpenWire.class.getDeclaredMethods()) {
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

    static ActiveMQConnectionFactory cf() {
        return cf("");
    }

    /** An OpenWire connection factory; options as in a broker URL's query. */
    static ActiveMQConnectionFactory cf(String options) {
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(ow + (options.isEmpty() ? "" : "?" + options));
        f.setWatchTopicAdvisories(true);
        return f;
    }

    static org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory core() {
        return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(url);
    }

    static void check(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static void eq(Object want, Object got, String what) {
        if (!Objects.equals(want, got)) throw new AssertionError(what + ": want <" + want + "> got <" + got + ">");
    }

    static String text(Message m) throws JMSException {
        check(m != null, "no message");
        return ((TextMessage) m).getText();
    }

    static void noMore(MessageConsumer c) throws JMSException {
        Message m = c.receive(200);
        check(m == null, "unexpected message " + m);
    }

    static Xid xid() {
        byte[] g = UUID.randomUUID().toString().getBytes();
        byte[] b = UUID.randomUUID().toString().getBytes();
        return new Xid() {
            public int getFormatId() { return 1; }
            public byte[] getGlobalTransactionId() { return g; }
            public byte[] getBranchQualifier() { return b; }
        };
    }

    static void waitFor(Callable<Boolean> cond, long millis, String what) throws Exception {
        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end) {
            if (cond.call()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("timed out: " + what);
    }

    /** Sends n text messages "<base><i>" with a property count. */
    static void sendTexts(Session s, Destination d, String base, int n) throws JMSException {
        MessageProducer p = s.createProducer(d);
        for (int i = 0; i < n; i++) {
            TextMessage m = s.createTextMessage(base + i);
            m.setStringProperty("count", "str " + i);
            p.send(m);
        }
        p.close();
    }

    // ---------------------------------------------------------------- sessions and acks

    static void t_manyTransactedSessions() throws Exception {
        try (Connection c = cf().createConnection()) {
            for (int i = 0; i < 10; i++) c.createSession(true, Session.SESSION_TRANSACTED);
        }
    }

    static void t_transactionalSimple() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("tx"));
            MessageProducer p = s.createProducer(q);
            MessageConsumer consumer = s.createConsumer(q);
            p.send(s.createTextMessage("test"));
            s.commit();
            check(consumer.receive(100) == null, "delivered before start");
            c.start();
            eq("test", text(consumer.receive(5000)), "body");
            s.commit();
        }
    }

    static void t_sendEmptyText() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("empty"));
            s.createProducer(q).send(s.createTextMessage());
            c.start();
            TextMessage m = (TextMessage) s.createConsumer(q).receive(5000);
            check(m != null, "no message");
            eq(null, m.getText(), "text");
        }
    }

    static void t_sendNullMapMessage() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("nullmap"));
            s.createProducer(q).send(s.createMapMessage());
            c.start();
            MapMessage m = (MapMessage) s.createConsumer(q).receive(5000);
            check(m != null, "no message");
            check(!m.getMapNames().hasMoreElements(), "names");
        }
    }

    static void t_sendEmptyMessagesOfEveryType() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("empties"));
            MessageProducer p = s.createProducer(q);
            p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            MessageConsumer r = s.createConsumer(q);
            Message[] all = {s.createMessage(), s.createBytesMessage(), s.createMapMessage(),
                s.createObjectMessage(), s.createStreamMessage(), s.createTextMessage()};
            for (Message m : all) {
                m.setStringProperty("testName", "testSendEmptyMessages");
                p.send(m);
                Message got = r.receive(2000);
                check(got != null, "no " + m.getClass().getSimpleName());
                eq(m.getClass(), got.getClass(), "type");
                eq("testSendEmptyMessages", got.getStringProperty("testName"), "property");
            }
        }
    }

    static void t_clientAck() throws Exception {
        String name = unique("clientack");
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(name);
            sendTexts(s, q, "m", 3);
            MessageConsumer consumer = s.createConsumer(q);
            c.start();
            text(consumer.receive(5000));
            text(consumer.receive(5000));
            Message last = consumer.receive(5000);
            text(last);
            last.acknowledge();
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            noMore(s.createConsumer(s.createQueue(name)));
        }
    }

    static void t_clientAckRecoverRedelivers() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("recover"));
            sendTexts(s, q, "m", 2);
            MessageConsumer consumer = s.createConsumer(q);
            c.start();
            Message first = consumer.receive(5000);
            eq("m0", text(first), "first");
            check(!first.getJMSRedelivered(), "redelivered at first");
            s.recover();
            Message again = consumer.receive(5000);
            eq("m0", text(again), "again");
            check(again.getJMSRedelivered(), "not redelivered");
            eq(2, again.getIntProperty("JMSXDeliveryCount"), "delivery count");
            again.acknowledge();
            eq("m1", text(consumer.receive(5000)), "second");
        }
    }

    static void t_closedConsumerRedeliversToNext() throws Exception {
        String name = unique("redeliver-next");
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(name);
            sendTexts(s, q, "m", 1);
            MessageConsumer consumer = s.createConsumer(q);
            c.start();
            eq("m0", text(consumer.receive(5000)), "first");
            consumer.close();
            s.close();
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message m = s.createConsumer(s.createQueue(name)).receive(5000);
            eq("m0", text(m), "redelivered");
            check(m.getJMSRedelivered(), "not marked redelivered");
        }
    }

    static void t_rollback() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("rollback"));
            MessageProducer p = s.createProducer(q);
            MessageConsumer consumer = s.createConsumer(q);
            p.send(s.createTextMessage("test"));
            p.send(s.createTextMessage("test2"));
            c.start();
            check(consumer.receiveNoWait() == null, "uncommitted delivered");
            s.rollback();
            p.send(s.createTextMessage("test2"));
            check(consumer.receiveNoWait() == null, "uncommitted delivered");
            s.commit();
            eq("test2", text(consumer.receive(1000)), "body");
        }
    }

    static void t_autoAck() throws Exception {
        String name = unique("autoack");
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name);
            MessageConsumer consumer = s.createConsumer(q);
            TextMessage msg = s.createTextMessage("test");
            msg.setStringProperty("abc", "testAutoACK");
            s.createProducer(q).send(msg);
            check(consumer.receive(100) == null, "before start");
            c.start();
            Message m = consumer.receive(5000);
            eq("test", text(m), "body");
            eq("testAutoACK", m.getStringProperty("abc"), "property");
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            noMore(s.createConsumer(s.createQueue(name)));
        }
    }

    static void t_dupsOkAck() throws Exception {
        String name = unique("dupsok");
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue q = s.createQueue(name);
            sendTexts(s, q, "m", 20);
            MessageConsumer consumer = s.createConsumer(q);
            c.start();
            for (int i = 0; i < 20; i++) eq("m" + i, text(consumer.receive(5000)), "body " + i);
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            noMore(s.createConsumer(s.createQueue(name)));
        }
    }

    static void t_producerFlowControlWindow() throws Exception {
        ActiveMQConnectionFactory f = cf();
        f.setProducerWindowSize(1024 * 64);
        try (Connection c = f.createConnection()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("window"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 50; i++) p.send(s.createTextMessage("x".repeat(4000)));
            s.commit();
        }
    }

    static void t_asyncSendsArrive() throws Exception {
        try (Connection c = cf("jms.useAsyncSend=true").createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("async"));
            MessageConsumer consumer = s.createConsumer(q);
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 10; i++) p.send(s.createTextMessage("testXX" + i));
            c.start();
            for (int i = 0; i < 10; i++) eq("testXX" + i, text(consumer.receive(5000)), "body");
        }
    }

    static void t_compressionOfEveryType() throws Exception {
        try (Connection cc = cf().createConnection(); Connection pc = cf("jms.useCompression=true").createConnection()) {
            cc.start();
            Session cs = cc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = cs.createQueue(unique("compressed"));
            MessageConsumer consumer = cs.createConsumer(q);
            Session s = pc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(q);
            p.setDeliveryMode(DeliveryMode.PERSISTENT);
            String body = "simple test string ".repeat(20);

            TextMessage t = s.createTextMessage(body);
            t.setStringProperty("BASE_DATE", "2017-11-01");
            p.send(t);
            TextMessage gt = (TextMessage) consumer.receive(2000);
            eq(body, gt.getText(), "text");
            eq("2017-11-01", gt.getStringProperty("BASE_DATE"), "property");

            MapMessage map = s.createMapMessage();
            map.setString("BASE_DATE", "2017-11-01");
            p.send(map);
            eq("2017-11-01", ((MapMessage) consumer.receive(2000)).getString("BASE_DATE"), "map");

            ObjectMessage obj = s.createObjectMessage(body);
            p.send(obj);
            eq(body, ((ObjectMessage) consumer.receive(2000)).getObject(), "object");

            StreamMessage stream = s.createStreamMessage();
            stream.writeString(body);
            p.send(stream);
            eq(body, ((StreamMessage) consumer.receive(2000)).readString(), "stream");

            BytesMessage bytes = s.createBytesMessage();
            bytes.writeBytes(body.getBytes());
            p.send(bytes);
            BytesMessage gb = (BytesMessage) consumer.receive(2000);
            eq((long) body.length(), gb.getBodyLength(), "body length");
            byte[] back = new byte[(int) gb.getBodyLength()];
            gb.readBytes(back);
            eq(body, new String(back), "bytes");

            p.send(s.createMessage());
            check(consumer.receive(2000) != null, "plain message");
        }
    }

    static void t_differentEncodings() throws Exception {
        String loose = "wireFormat.tightEncodingEnabled=false";
        for (String[] pair : new String[][] {{"", loose}, {loose, ""}, {"wireFormat.cacheEnabled=false", ""},
                {"", "wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false"}}) {
            try (Connection pc = cf(pair[0]).createConnection(); Connection cc = cf(pair[1]).createConnection()) {
                cc.start();
                Session ps = pc.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue q = ps.createQueue(unique("encoding"));
                sendTexts(ps, q, "MfromAMQ-", 10);
                Session cs = cc.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = cs.createConsumer(q);
                for (int i = 0; i < 10; i++) {
                    Message m = consumer.receive(5000);
                    eq("MfromAMQ-" + i, text(m), Arrays.toString(pair));
                    eq("str " + i, m.getStringProperty("count"), "property");
                }
                noMore(consumer);
            }
        }
    }

    static void t_messageHeadersAndProperties() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("headers"));
            Queue reply = s.createQueue(unique("reply"));
            MessageProducer p = s.createProducer(q);
            TextMessage m = s.createTextMessage("headers");
            m.setJMSCorrelationID("corr-1");
            m.setJMSType("my-type");
            m.setJMSReplyTo(reply);
            m.setBooleanProperty("b", true);
            m.setByteProperty("by", (byte) 7);
            m.setShortProperty("sh", (short) 300);
            m.setIntProperty("i", 42);
            m.setLongProperty("l", 1L << 40);
            m.setFloatProperty("f", 1.5f);
            m.setDoubleProperty("d", 2.25);
            m.setStringProperty("s", "héllo");
            m.setStringProperty("JMSXGroupID", "group-1");
            p.send(m, DeliveryMode.NON_PERSISTENT, 7, 60000);
            Message got = s.createConsumer(q).receive(5000);
            eq("headers", text(got), "body");
            eq("corr-1", got.getJMSCorrelationID(), "correlation");
            eq("my-type", got.getJMSType(), "type");
            eq(reply, got.getJMSReplyTo(), "reply to");
            eq(DeliveryMode.NON_PERSISTENT, got.getJMSDeliveryMode(), "delivery mode");
            eq(7, got.getJMSPriority(), "priority");
            check(got.getJMSExpiration() > System.currentTimeMillis(), "expiration");
            eq(m.getJMSMessageID(), got.getJMSMessageID(), "message id");
            eq(true, got.getBooleanProperty("b"), "boolean");
            eq((byte) 7, got.getByteProperty("by"), "byte");
            eq((short) 300, got.getShortProperty("sh"), "short");
            eq(42, got.getIntProperty("i"), "int");
            eq(1L << 40, got.getLongProperty("l"), "long");
            eq(1.5f, got.getFloatProperty("f"), "float");
            eq(2.25, got.getDoubleProperty("d"), "double");
            eq("héllo", got.getStringProperty("s"), "string");
            eq("group-1", got.getStringProperty("JMSXGroupID"), "group");
        }
    }

    static void t_messageBodies() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("bodies"));
            MessageProducer p = s.createProducer(q);
            MessageConsumer consumer = s.createConsumer(q);

            MapMessage map = s.createMapMessage();
            map.setString("name", "widget");
            map.setInt("count", 3);
            map.setBytes("raw", new byte[] {1, 2});
            map.setChar("ch", 'z');
            p.send(map);
            MapMessage gm = (MapMessage) consumer.receive(5000);
            eq("widget", gm.getString("name"), "map string");
            eq(3, gm.getInt("count"), "map int");
            eq(2, gm.getBytes("raw").length, "map bytes");
            eq('z', gm.getChar("ch"), "map char");

            StreamMessage stream = s.createStreamMessage();
            stream.writeBoolean(true);
            stream.writeInt(5);
            stream.writeString("s");
            stream.writeBytes(new byte[] {9});
            stream.writeDouble(1.25);
            p.send(stream);
            StreamMessage gs = (StreamMessage) consumer.receive(5000);
            eq(true, gs.readBoolean(), "stream boolean");
            eq(5, gs.readInt(), "stream int");
            eq("s", gs.readString(), "stream string");
            byte[] b = new byte[1];
            eq(1, gs.readBytes(b), "stream bytes");
            eq(1.25, gs.readDouble(), "stream double");

            BytesMessage bytes = s.createBytesMessage();
            bytes.writeUTF("utf");
            bytes.writeLong(77);
            p.send(bytes);
            BytesMessage gb = (BytesMessage) consumer.receive(5000);
            eq("utf", gb.readUTF(), "bytes utf");
            eq(77L, gb.readLong(), "bytes long");

            p.send(s.createObjectMessage("serialized"));
            eq("serialized", ((ObjectMessage) consumer.receive(5000)).getObject(), "object");
        }
    }

    static void t_largeMessage() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("large"));
            byte[] data = new byte[2 * 1024 * 1024];
            new Random(1).nextBytes(data);
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(data);
            s.createProducer(q).send(m);
            BytesMessage got = (BytesMessage) s.createConsumer(q).receive(10000);
            check(got != null, "no message");
            byte[] back = new byte[(int) got.getBodyLength()];
            got.readBytes(back);
            check(Arrays.equals(data, back), "body differs");
        }
    }

    // ---------------------------------------------------------------- queues and topics

    static void t_simpleQueue() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination d = new ActiveMQQueue(unique("simple"));
            sendTexts(s, d, "MfromAMQ-", 1);
            MessageConsumer consumer = s.createConsumer(d);
            eq("MfromAMQ-0", text(consumer.receive(5000)), "body");
            noMore(consumer);
        }
    }

    static void t_simpleTopic() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination d = new ActiveMQTopic(unique("topic"));
            MessageConsumer c1 = s.createConsumer(d);
            MessageConsumer c2 = s.createConsumer(d);
            sendTexts(s, d, "MfromAMQ-", 1);
            eq("MfromAMQ-0", text(c1.receive(5000)), "c1");
            eq("MfromAMQ-0", text(c2.receive(5000)), "c2");
            noMore(c1);
            noMore(c2);
        }
    }

    static void t_topicNoLocal() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination d = new ActiveMQTopic(unique("nolocal"));
            MessageConsumer nolocal = s.createConsumer(d, null, true);
            MessageConsumer normal = s.createConsumer(d, null, false);
            MessageConsumer selector = s.createConsumer(d, "TESTKEY = 'test'", false);
            MessageProducer p = s.createProducer(d);
            p.send(s.createTextMessage("MfromAMQ-1"));
            TextMessage m = s.createTextMessage("MfromAMQ-2");
            m.setStringProperty("TESTKEY", "test");
            p.send(m);
            check(nolocal.receive(1000) == null, "nolocal got a local message");
            eq("MfromAMQ-1", text(normal.receive(1000)), "normal 1");
            eq("MfromAMQ-2", text(normal.receive(1000)), "normal 2");
            check(normal.receiveNoWait() == null, "normal got more");
            eq("MfromAMQ-2", text(selector.receive(1000)), "selector");
            check(selector.receiveNoWait() == null, "selector got more");
            try (Connection other = cf().createConnection()) {
                Session os = other.createSession(false, Session.AUTO_ACKNOWLEDGE);
                os.createProducer(d).send(os.createTextMessage("MfromAMQ-1"));
                check(normal.receive(1000) != null, "normal from other");
                check(selector.receive(500) == null, "selector from other");
                check(nolocal.receive(1000) != null, "nolocal from other");
            }
        }
    }

    static void t_durableSubscriptions() throws Exception {
        String client = unique("client");
        Topic d = new ActiveMQTopic(unique("durable"));
        try (Connection c = cf().createConnection()) {
            c.setClientID(client);
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer nolocal = s.createDurableSubscriber(d, "nolocal-subscriber1", "", true);
            MessageConsumer normal = s.createDurableSubscriber(d, "normal-subscriber", null, false);
            MessageConsumer selector = s.createDurableSubscriber(d, "selector-subscriber", "TESTKEY = 'test'", false);
            MessageProducer p = s.createProducer(d);
            p.send(s.createTextMessage("MfromAMQ-1"));
            TextMessage m = s.createTextMessage("MfromAMQ-2");
            m.setStringProperty("TESTKEY", "test");
            p.send(m);
            check(nolocal.receive(1000) == null, "nolocal got a local message");
            eq("MfromAMQ-1", text(normal.receive(1000)), "normal 1");
            eq("MfromAMQ-2", text(normal.receive(1000)), "normal 2");
            eq("MfromAMQ-2", text(selector.receive(1000)), "selector");
            normal.close();
            // Offline, the subscription keeps what is published.
            try (Connection other = cf().createConnection()) {
                Session os = other.createSession(false, Session.AUTO_ACKNOWLEDGE);
                os.createProducer(d).send(os.createTextMessage("offline"));
            }
            check(nolocal.receive(1000) != null, "nolocal from other");
        }
        try (Connection c = cf().createConnection()) {
            c.setClientID(client);
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer normal = s.createDurableSubscriber(d, "normal-subscriber");
            eq("offline", text(normal.receive(5000)), "kept while offline");
            normal.close();
            s.unsubscribe("normal-subscriber");
            s.unsubscribe("nolocal-subscriber1");
            s.unsubscribe("selector-subscriber");
            try {
                s.unsubscribe("never-made");
                throw new AssertionError("unsubscribed an unknown subscription");
            } catch (InvalidDestinationException expected) {
            }
        }
    }

    static void t_unsubscribeActiveSubscriptionFails() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.setClientID(unique("client"));
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic d = new ActiveMQTopic(unique("active"));
            MessageConsumer sub = s.createDurableSubscriber(d, "sub");
            try {
                s.unsubscribe("sub");
                throw new AssertionError("unsubscribed an active subscription");
            } catch (JMSException expected) {
            }
            sub.close();
            s.unsubscribe("sub");
        }
    }

    static void t_queueBrowser() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("browse"));
            sendTexts(s, q, "m", 5);
            QueueBrowser b = s.createBrowser(q);
            Enumeration<?> e = b.getEnumeration();
            int n = 0;
            while (e.hasMoreElements()) {
                eq("m" + n, text((Message) e.nextElement()), "browsed");
                n++;
            }
            eq(5, n, "browsed count");
            b.close();
            QueueBrowser filtered = s.createBrowser(q, "count = 'str 3'");
            Enumeration<?> fe = filtered.getEnumeration();
            check(fe.hasMoreElements(), "filtered browse empty");
            eq("m3", text((Message) fe.nextElement()), "filtered");
            check(!fe.hasMoreElements(), "filtered browse has more");
            MessageConsumer consumer = s.createConsumer(q);
            for (int i = 0; i < 5; i++) eq("m" + i, text(consumer.receive(5000)), "still there");
        }
    }

    static void t_selectorOnQueue() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("selector"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 6; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setIntProperty("n", i);
                p.send(m);
            }
            MessageConsumer even = s.createConsumer(q, "n = 0 OR n = 2 OR n = 4");
            for (int i = 0; i < 6; i += 2) eq("m" + i, text(even.receive(5000)), "even");
            noMore(even);
            MessageConsumer rest = s.createConsumer(q);
            for (int i = 1; i < 6; i += 2) eq("m" + i, text(rest.receive(5000)), "odd");
        }
    }

    static void t_prefetchZeroPulls() throws Exception {
        try (Connection c = cf("jms.prefetchPolicy.all=0").createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("pull"));
            MessageConsumer consumer = s.createConsumer(q);
            check(consumer.receiveNoWait() == null, "empty queue");
            check(consumer.receive(300) == null, "empty queue with timeout");
            sendTexts(s, q, "m", 3);
            eq("m0", text(consumer.receive(5000)), "pull 0");
            eq("m1", text(consumer.receive(5000)), "pull 1");
            // Nothing was prefetched: a second consumer gets the last one.
            MessageConsumer other = s.createConsumer(q);
            eq("m2", text(other.receive(5000)), "other pulls");
            check(consumer.receiveNoWait() == null, "nothing left");
        }
    }

    static void t_prefetchOneSharesQueue() throws Exception {
        try (Connection c = cf("jms.prefetchPolicy.queuePrefetch=1").createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("prefetch1"));
            sendTexts(s, q, "m", 4);
            MessageConsumer a = s.createConsumer(q);
            MessageConsumer b = s.createConsumer(q);
            Message ma = a.receive(5000);
            Message mb = b.receive(5000);
            check(ma != null && mb != null, "both consumers got one");
            check(!text(ma).equals(text(mb)), "same message twice");
            ma.acknowledge();
        }
    }

    static void t_messageGroups() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("groups"));
            MessageConsumer a = s.createConsumer(q);
            MessageConsumer b = s.createConsumer(q);
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 6; i++) {
                TextMessage m = s.createTextMessage("g" + i);
                m.setStringProperty("JMSXGroupID", "group-A");
                p.send(m);
            }
            int ga = 0, gb = 0;
            for (int i = 0; i < 6; i++) {
                Message m = a.receive(300);
                if (m != null) ga++;
                else if (b.receive(1000) != null) gb++;
            }
            check(ga == 6 || gb == 6, "group split: " + ga + "/" + gb);
        }
    }

    static void t_compositeDestinations() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String a = unique("compA"), b = unique("compB"), t = unique("compT");
            MessageConsumer ca = s.createConsumer(s.createQueue(a));
            MessageConsumer cb = s.createConsumer(s.createQueue(b));
            MessageConsumer ct = s.createConsumer(s.createTopic(t));
            s.createProducer(new ActiveMQQueue(a + "," + b + ",topic://" + t)).send(s.createTextMessage("everywhere"));
            eq("everywhere", text(ca.receive(5000)), "a");
            eq("everywhere", text(cb.receive(5000)), "b");
            eq("everywhere", text(ct.receive(5000)), "topic");
        }
    }

    static void t_wildcardTopicConsumer() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String base = unique("news");
            MessageConsumer all = s.createConsumer(s.createTopic(base + ".>"));
            MessageConsumer one = s.createConsumer(s.createTopic(base + ".*.sport"));
            s.createProducer(s.createTopic(base + ".uk.sport")).send(s.createTextMessage("goal"));
            s.createProducer(s.createTopic(base + ".uk.weather")).send(s.createTextMessage("rain"));
            Set<String> got = new HashSet<>();
            got.add(text(all.receive(5000)));
            got.add(text(all.receive(5000)));
            eq(Set.of("goal", "rain"), got, "all");
            eq("goal", text(one.receive(5000)), "sport");
            noMore(one);
        }
    }

    static void t_virtualTopicConsumers() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String topic = "VirtualTopic." + unique("Orders");
            MessageConsumer a1 = s.createConsumer(s.createQueue("Consumer.A." + topic));
            MessageConsumer a2 = s.createConsumer(s.createQueue("Consumer.A." + topic));
            MessageConsumer b = s.createConsumer(s.createQueue("Consumer.B." + topic));
            MessageProducer p = s.createProducer(s.createTopic(topic));
            for (int i = 0; i < 4; i++) p.send(s.createTextMessage("o" + i));
            int ga = 0;
            for (int i = 0; i < 4; i++) {
                if (a1.receive(500) != null) ga++;
                else if (a2.receive(500) != null) ga++;
            }
            eq(4, ga, "group A shares");
            for (int i = 0; i < 4; i++) eq("o" + i, text(b.receive(5000)), "group B");
        }
    }

    static void t_scheduledDelay() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("scheduled"));
            TextMessage m = s.createTextMessage("later");
            m.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 1500);
            long sent = System.currentTimeMillis();
            s.createProducer(q).send(m);
            MessageConsumer consumer = s.createConsumer(q);
            check(consumer.receive(700) == null, "delivered early");
            eq("later", text(consumer.receive(5000)), "body");
            check(System.currentTimeMillis() - sent >= 1400, "not delayed");
        }
    }

    static void t_expiredNotDelivered() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("expiry"));
            MessageProducer p = s.createProducer(q);
            p.setTimeToLive(100);
            p.send(s.createTextMessage("short-lived"));
            Thread.sleep(400);
            c.start();
            noMore(s.createConsumer(q));
        }
    }

    // ---------------------------------------------------------------- redelivery and DLQ

    static void t_poisonAckGoesToDlq() throws Exception {
        String name = "redeliver." + unique("poison");
        ActiveMQConnectionFactory f = cf();
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(1);
        policy.setInitialRedeliveryDelay(0);
        f.setRedeliveryPolicy(policy);
        try (Connection c = f.createConnection()) {
            c.start();
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(name);
            TextMessage msg = s.createTextMessage("poison");
            msg.setStringProperty("mark", name);
            s.createProducer(q).send(msg);
            s.commit();
            MessageConsumer consumer = s.createConsumer(q);
            for (int i = 0; i < 2; i++) {
                eq("poison", text(consumer.receive(5000)), "attempt " + i);
                s.rollback();
            }
            check(consumer.receive(500) == null, "still delivered after max redeliveries");
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer dlq = s.createConsumer(s.createQueue("DLQ"), "mark = '" + name + "'");
            Message m = dlq.receive(5000);
            eq("poison", text(m), "in DLQ");
            String cause = m.getStringProperty("dlqDeliveryFailureCause");
            check(cause != null && cause.contains("cause:"), "failure cause: " + cause);
        }
    }

    static void t_rollbackWithAcked() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("rollback-acked"));
            MessageConsumer consumer = s.createConsumer(q);
            sendTexts(s, q, "testXX", 10);
            s.commit();
            for (int i = 0; i < 5; i++) eq("testXX" + i, text(consumer.receive(5000)), "first pass");
            s.rollback();
            consumer.close();
            consumer = s.createConsumer(q);
            for (int i = 0; i < 10; i++) check(consumer.receive(5000) != null, "second pass " + i);
            s.commit();
            noMore(consumer);
        }
    }

    static void t_rollbackLocal() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("rollback-local"));
            MessageConsumer consumer = s.createConsumer(q);
            sendTexts(s, q, "testXX", 10);
            s.commit();
            for (int i = 0; i < 5; i++) eq("testXX" + i, text(consumer.receive(500)), "first pass");
            s.rollback();
            for (int i = 0; i < 10; i++) eq("testXX" + i, text(consumer.receive(5000)), "second pass");
            noMore(consumer);
            s.commit();
        }
    }

    static void commitCloseConsumer(boolean closeBefore) throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(unique("commit-close"));
            MessageConsumer consumer = s.createConsumer(q);
            sendTexts(s, q, "testXX", 10);
            s.commit();
            for (int i = 0; i < 5; i++) eq("testXX" + i, text(consumer.receive(5000)), "first");
            if (closeBefore) consumer.close();
            s.commit();
            if (!closeBefore) consumer.close();
            consumer = s.createConsumer(q);
            for (int i = 5; i < 10; i++) eq("testXX" + i, text(consumer.receive(5000)), "rest");
            check(consumer.receiveNoWait() == null, "more");
        }
    }

    static void t_commitCloseConsumerBefore() throws Exception {
        commitCloseConsumer(true);
    }

    static void t_commitCloseConsumerAfter() throws Exception {
        commitCloseConsumer(false);
    }

    static void t_individualAck() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("individual"));
            MessageConsumer consumer = s.createConsumer(q);
            sendTexts(s, q, "testXX", 10);
            for (int i = 0; i < 5; i++) {
                Message m = consumer.receive(5000);
                if (i == 4) m.acknowledge();
                eq("testXX" + i, text(m), "first");
            }
            consumer.close();
            consumer = s.createConsumer(q);
            for (int i = 0; i < 4; i++) {
                Message m = consumer.receive(5000);
                m.acknowledge();
                eq("testXX" + i, text(m), "redelivered");
            }
            for (int i = 5; i < 10; i++) {
                Message m = consumer.receive(5000);
                m.acknowledge();
                eq("testXX" + i, text(m), "rest");
            }
            check(consumer.receiveNoWait() == null, "more");
        }
    }

    // ---------------------------------------------------------------- XA

    static void t_xaSessions() throws Exception {
        try (XAConnection c = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            for (int i = 0; i < 10; i++) c.createXASession().getXAResource().start(xid(), XAResource.TMNOFLAGS);
        }
    }

    static void t_xaConsumerRollback() throws Exception {
        Queue q;
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            q = s.createQueue(unique("xa-consumer"));
            sendTexts(s, q, "test", 10);
        }
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            Xid x = xid();
            XASession xs = xc.createXASession();
            xs.getXAResource().start(x, XAResource.TMNOFLAGS);
            MessageConsumer consumer = xs.createConsumer(q);
            xc.start();
            for (int i = 0; i < 5; i++) eq("test" + i, text(consumer.receive(5000)), "xa");
            xs.getXAResource().end(x, XAResource.TMSUCCESS);
            xs.getXAResource().rollback(x);
            consumer.close();
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(q);
            for (int i = 0; i < 10; i++) check(consumer.receive(5000) != null, "after rollback " + i);
            noMore(consumer);
        }
    }

    static void t_xaSameConsumerRollback() throws Exception {
        Queue q;
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            q = s.createQueue(unique("xa-same"));
            sendTexts(s, q, "test", 10);
        }
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            Xid x = xid();
            XASession xs = xc.createXASession();
            xs.getXAResource().start(x, XAResource.TMNOFLAGS);
            MessageConsumer consumer = xs.createConsumer(q);
            xc.start();
            for (int i = 0; i < 5; i++) eq("test" + i, text(consumer.receive(5000)), "first");
            xs.getXAResource().end(x, XAResource.TMSUCCESS);
            xs.getXAResource().rollback(x);
            x = xid();
            xs.getXAResource().start(x, XAResource.TMNOFLAGS);
            for (int i = 0; i < 10; i++) eq("test" + i, text(consumer.receive(5000)), "again");
            check(consumer.receiveNoWait() == null, "duplicates");
            xs.getXAResource().end(x, XAResource.TMSUCCESS);
            xs.getXAResource().commit(x, true);
        }
    }

    static void t_xaTwoPhaseCommitAndRecover() throws Exception {
        String name = unique("xa-2pc");
        Xid x = xid();
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            XASession xs = xc.createXASession();
            XAResource r = xs.getXAResource();
            r.start(x, XAResource.TMNOFLAGS);
            Queue q = xs.createQueue(name);
            MessageProducer p = xs.createProducer(q);
            p.send(xs.createTextMessage("hello"));
            p.send(xs.createTextMessage("hello"));
            r.end(x, XAResource.TMSUCCESS);
            eq(XAResource.XA_OK, r.prepare(x), "prepare");
        }
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            XAResource r = xc.createXASession().getXAResource();
            Xid[] prepared = r.recover(XAResource.TMSTARTRSCAN);
            boolean found = false;
            for (Xid p : prepared) found |= Arrays.equals(p.getGlobalTransactionId(), x.getGlobalTransactionId());
            check(found, "prepared xid not recovered");
            r.commit(x, false);
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(name));
            eq("hello", text(consumer.receive(5000)), "committed 1");
            eq("hello", text(consumer.receive(5000)), "committed 2");
        }
    }

    static void t_xaCommitCloseConsume() throws Exception {
        Queue q;
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            q = s.createQueue(unique("xa-close"));
            sendTexts(s, q, "testXX", 10);
            s.commit();
        }
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            xc.start();
            XASession xs = xc.createXASession();
            Xid x = xid();
            xs.getXAResource().start(x, XAResource.TMNOFLAGS);
            MessageConsumer consumer = xs.createConsumer(q);
            for (int i = 0; i < 5; i++) eq("testXX" + i, text(consumer.receive(5000)), "xa");
            consumer.close();
            xs.getXAResource().end(x, XAResource.TMSUCCESS);
            xs.getXAResource().prepare(x);
            xs.getXAResource().commit(x, false);
        }
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = s.createConsumer(q);
            for (int i = 5; i < 10; i++) eq("testXX" + i, text(consumer.receive(5000)), "rest");
            s.commit();
        }
    }

    static void t_xaCommitUnknownFails() throws Exception {
        try (XAConnection xc = new ActiveMQXAConnectionFactory(ow).createXAConnection()) {
            XAResource r = xc.createXASession().getXAResource();
            try {
                r.commit(xid(), true);
                throw new AssertionError("committed an unknown xid");
            } catch (XAException expected) {
            }
        }
    }

    // ---------------------------------------------------------------- temporary destinations

    static void t_simpleTempTopic() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryTopic t = s.createTemporaryTopic();
            MessageConsumer c1 = s.createConsumer(t);
            MessageConsumer c2 = s.createConsumer(t);
            sendTexts(s, t, "MfromAMQ-", 1);
            eq("MfromAMQ-0", text(c1.receive(5000)), "c1");
            eq("MfromAMQ-0", text(c2.receive(5000)), "c2");
        }
    }

    static void t_simpleTempQueue() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue t = s.createTemporaryQueue();
            MessageConsumer c1 = s.createConsumer(t);
            sendTexts(s, t, "MfromAMQ-", 1);
            eq("MfromAMQ-0", text(c1.receive(5000)), "c1");
            noMore(c1);
            for (int i = 0; i < 10; i++) s.createProducer(t).close();
        }
    }

    static void t_tempQueueOfAnotherConnection() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue t = s.createTemporaryQueue();
            ActiveMQConnection other = (ActiveMQConnection) cf().createConnection();
            try {
                other.start();
                Session os = other.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer p = os.createProducer(t);
                waitFor(() -> !other.isDeleted((ActiveMQDestination) t), 5000, "advisory of the temp queue");
                p.send(os.createTextMessage("to temp"));
                try {
                    os.createConsumer(t);
                    throw new AssertionError("consumed another connection's temp queue");
                } catch (JMSException expected) {
                }
                eq("to temp", text(s.createConsumer(t).receive(5000)), "owner receives");
                c.close();
                waitFor(() -> other.isDeleted((ActiveMQDestination) t), 5000, "advisory of the deletion");
                try {
                    p.send(os.createTextMessage("too late"));
                    throw new AssertionError("sent to a deleted temp queue");
                } catch (InvalidDestinationException expected) {
                }
            } finally {
                other.close();
            }
        }
    }

    static void t_deleteTempQueueInUseFails() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue t = s.createTemporaryQueue();
            MessageConsumer consumer = s.createConsumer(t);
            try {
                t.delete();
                throw new AssertionError("deleted a temp queue in use");
            } catch (JMSException expected) {
            }
            consumer.close();
            t.delete();
        }
    }

    // ---------------------------------------------------------------- destinations

    static void t_producerOnMissingQueueFails() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                s.createProducer(s.createQueue("noauto." + unique("missing")));
                throw new AssertionError("producer on a missing queue");
            } catch (JMSException expected) {
            }
        }
    }

    static void t_sendToMissingQueueFails() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(null);
            try {
                p.send(s.createQueue("noauto." + unique("missing")), s.createTextMessage("x"));
                throw new AssertionError("sent to a missing queue");
            } catch (InvalidDestinationException expected) {
            }
            p.send(s.createQueue("noauto.existing"), s.createTextMessage("existing"));
            c.start();
            eq("existing", text(s.createConsumer(s.createQueue("noauto.existing")).receive(5000)), "existing");
        }
    }

    static void t_consumerOnMissingQueueFails() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                s.createConsumer(s.createQueue("noauto." + unique("missing")));
                throw new AssertionError("consumer on a missing queue");
            } catch (JMSException expected) {
            }
        }
    }

    static boolean coreQueueExists(String name) throws Exception {
        try (org.apache.activemq.artemis.api.core.client.ServerLocator locator =
                 org.apache.activemq.artemis.api.core.client.ActiveMQClient.createServerLocator(url);
             org.apache.activemq.artemis.api.core.client.ClientSessionFactory sf = locator.createSessionFactory();
             org.apache.activemq.artemis.api.core.client.ClientSession cs = sf.createSession()) {
            return cs.queueQuery(org.apache.activemq.artemis.api.core.SimpleString.of(name)).isExists();
        }
    }

    static void t_autoCreatedQueueIsDeletedAfterUse() throws Exception {
        String name = unique("autodelete");
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = new ActiveMQQueue(name);
            MessageConsumer consumer = s.createConsumer(q);
            waitFor(() -> coreQueueExists(name), 2000, "auto-created");
            s.createProducer(null).send(q, s.createTextMessage("bar"));
            eq("bar", text(consumer.receive(1000)), "body");
            consumer.close();
        }
        waitFor(() -> !coreQueueExists(name), 5000, "auto-deleted");
    }

    // ---------------------------------------------------------------- advisories

    static void t_connectionAdvisory() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer advisories = s.createConsumer(AdvisorySupport.getConnectionAdvisoryTopic());
            try (Connection other = cf().createConnection()) {
                other.setClientID(unique("watched"));
                other.start();
                String clientId = other.getClientID();
                boolean seen = false;
                for (int i = 0; i < 10 && !seen; i++) {
                    Message m = advisories.receive(2000);
                    check(m != null, "no connection advisory");
                    DataStructure ds = ((ActiveMQMessage) m).getDataStructure();
                    seen = ds instanceof ConnectionInfo info && clientId.equals(info.getClientId());
                }
                check(seen, "advisory of the new connection");
            }
        }
    }

    static void t_tempQueueAdvisory() throws Exception {
        try (Connection c = cf().createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer advisories = s.createConsumer(AdvisorySupport.TEMP_QUEUE_ADVISORY_TOPIC);
            try (Connection other = cf().createConnection()) {
                TemporaryQueue t = other.createSession(false, Session.AUTO_ACKNOWLEDGE).createTemporaryQueue();
                boolean seen = false;
                for (int i = 0; i < 10 && !seen; i++) {
                    Message m = advisories.receive(2000);
                    check(m != null, "no temp queue advisory");
                    DataStructure ds = ((ActiveMQMessage) m).getDataStructure();
                    seen = ds instanceof org.apache.activemq.command.DestinationInfo info
                        && t.getQueueName().equals(info.getDestination().getPhysicalName());
                }
                check(seen, "advisory of the temp queue");
            }
        }
    }

    // ---------------------------------------------------------------- mixed protocols

    static void t_openWireToCore() throws Exception {
        String name = unique("ow-to-core");
        try (Connection oc = cf().createConnection()) {
            Session s = oc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TextMessage m = s.createTextMessage("This is a text message");
            m.setStringProperty("prop", "value");
            m.setJMSCorrelationID("corr");
            s.createProducer(s.createQueue(name)).send(m);
        }
        try (Connection cc = core().createConnection()) {
            cc.start();
            Session s = cc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message m = s.createConsumer(s.createQueue(name)).receive(5000);
            eq("This is a text message", text(m), "body");
            eq("value", m.getStringProperty("prop"), "property");
            eq("corr", m.getJMSCorrelationID(), "correlation");
        }
    }

    static void t_coreToOpenWire() throws Exception {
        String name = unique("core-to-ow");
        try (Connection cc = core().createConnection()) {
            Session s = cc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(name));
            for (int i = 0; i < 10; i++) {
                TextMessage m = s.createTextMessage("core " + i);
                m.setIntProperty("i", i);
                p.send(m);
            }
            MapMessage map = s.createMapMessage();
            map.setString("k", "v");
            p.send(map);
            BytesMessage bytes = s.createBytesMessage();
            bytes.writeInt(99);
            p.send(bytes);
        }
        try (Connection oc = cf().createConnection()) {
            oc.start();
            Session s = oc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(name));
            for (int i = 0; i < 10; i++) {
                Message m = consumer.receive(5000);
                eq("core " + i, text(m), "body");
                eq(i, m.getIntProperty("i"), "property");
                check(m.getJMSMessageID() != null && m.getJMSMessageID().startsWith("ID:"), "message id " + m.getJMSMessageID());
            }
            eq("v", ((MapMessage) consumer.receive(5000)).getString("k"), "map");
            eq(99, ((BytesMessage) consumer.receive(5000)).readInt(), "bytes");
        }
    }

    static void t_propertyConversions() throws Exception {
        try (Connection c = cf().createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("conversions"));
            TextMessage m = s.createTextMessage("This is a text message");
            m.setStringProperty("__HDR_BROKER_PATH", UUID.randomUUID().toString());
            m.setStringProperty("__HDR_CLUSTER", UUID.randomUUID().toString());
            m.setStringProperty("__HDR_USER_ID", UUID.randomUUID().toString());
            s.createProducer(q).send(m);
            c.start();
            check(s.createConsumer(q).receive(5000) != null, "no message");
        }
    }

    static void t_keepAliveKeepsIdleConnection() throws Exception {
        try (Connection c = cf("wireFormat.maxInactivityDuration=5000&wireFormat.maxInactivityDurationInitalDelay=1000").createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(unique("idle"));
            Thread.sleep(12000);
            s.createProducer(q).send(s.createTextMessage("still here"));
            eq("still here", text(s.createConsumer(q).receive(5000)), "after idling");
        }
    }

    static void t_duplicateClientIdRefused() throws Exception {
        String id = unique("dup-client");
        try (Connection c = cf().createConnection()) {
            c.setClientID(id);
            c.start();
            try (Connection other = cf().createConnection()) {
                other.setClientID(id);
                other.start();
                throw new AssertionError("second connection with the same client ID");
            } catch (InvalidClientIDException expected) {
            }
        }
    }

    // ------------------------------------------------------------ full addresses

    static final String PAD = "x".repeat(1000);

    /** Sends 300 texts of about 1K to q on its own thread, counting them. */
    static Thread producing(Connection c, String q, int mode, AtomicInteger sent, AtomicReference<Throwable> failure) {
        Thread t = new Thread(() -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setDeliveryMode(mode);
                for (int i = 0; i < 300; i++) {
                    TextMessage m = s.createTextMessage(i + "-" + PAD);
                    m.setIntProperty("i", i);
                    p.send(m);
                    sent.incrementAndGet();
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        t.start();
        return t;
    }

    /** Takes n messages from q in order. */
    static void takeInOrder(Connection c, String q, int n) throws JMSException {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            for (int i = 0; i < n; i++) {
                Message m = mc.receive(10000);
                check(m != null, "message " + i);
                eq(i, m.getIntProperty("i"), "in order");
            }
            noMore(mc);
        }
    }

    // A sync send to a full BLOCK address waits for room (sendShouldBlockProducer).
    static void t_blockSyncSend() throws Exception {
        String q = unique("ow.block.sync");
        try (Connection c = cf().createConnection()) {
            c.start();
            AtomicInteger sent = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = producing(c, q, DeliveryMode.PERSISTENT, sent, failure);
            Thread.sleep(1500);
            check(producer.isAlive(), "the producer is blocked");
            check(sent.get() < 300, "not everything was sent: " + sent.get());
            takeInOrder(c, q, 300);
            producer.join(10000);
            check(failure.get() == null, "producer failed: " + failure.get());
            eq(300, sent.get(), "all sent once unblocked");
        }
    }

    // Async sends with a producer window wait for their ProducerAcks, held
    // back while the address is full. Those held back race the ones sent
    // once there is room, so they may arrive out of order, as on Artemis,
    // and are dropped if their session closes first (blockedRunnables), so
    // the producer's session stays open until they are taken.
    static void t_blockProducerWindow() throws Exception {
        String q = unique("ow.block.window");
        try (Connection c = cf("jms.producerWindowSize=65536").createConnection()) {
            c.start();
            AtomicInteger sent = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch taken = new CountDownLatch(1);
            Thread producer = new Thread(() -> {
                try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageProducer p = s.createProducer(s.createQueue(q));
                    p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    for (int i = 0; i < 300; i++) {
                        TextMessage m = s.createTextMessage(i + "-" + PAD);
                        m.setIntProperty("i", i);
                        p.send(m);
                        sent.incrementAndGet();
                    }
                    taken.await(30, TimeUnit.SECONDS);
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            producer.start();
            Thread.sleep(1500);
            check(producer.isAlive(), "the producer is blocked");
            check(sent.get() < 300, "not everything was sent: " + sent.get());
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                Set<Integer> seen = new HashSet<>();
                for (int i = 0; i < 300; i++) {
                    Message m = mc.receive(10000);
                    check(m != null, "message " + i);
                    check(seen.add(m.getIntProperty("i")), "once each: " + m.getIntProperty("i"));
                }
                noMore(mc);
                taken.countDown();
            }
            producer.join(10000);
            check(failure.get() == null, "producer failed: " + failure.get());
            eq(300, sent.get(), "all sent once unblocked");
        }
    }

    // Async sends without a window are taken, but the connection is not read
    // while the address is full: a sync send after them waits.
    static void t_blockAsyncStopsReading() throws Exception {
        String q = unique("ow.block.async");
        String other = unique("ow.other");
        try (Connection c = cf().createConnection()) {
            c.start();
            AtomicInteger sent = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean answered = new AtomicBoolean();
            Thread producer = new Thread(() -> {
                try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageProducer p = s.createProducer(s.createQueue(q));
                    p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    for (int i = 0; i < 300; i++) {
                        TextMessage m = s.createTextMessage(i + "-" + PAD);
                        m.setIntProperty("i", i);
                        p.send(m);
                        sent.incrementAndGet();
                    }
                    MessageProducer sync = s.createProducer(s.createQueue(other));
                    sync.setDeliveryMode(DeliveryMode.PERSISTENT);
                    sync.send(s.createTextMessage("after"));
                    answered.set(true);
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            producer.start();
            Thread.sleep(1500);
            check(failure.get() == null, "producer failed: " + failure.get());
            check(!answered.get(), "the sync send after them waits");
            try (Connection c2 = cf().createConnection()) {
                c2.start();
                takeInOrder(c2, q, 300);
            }
            producer.join(10000);
            check(failure.get() == null, "producer failed: " + failure.get());
            check(answered.get(), "the sync send was answered once the address had room");
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                eq("after", text(s.createConsumer(s.createQueue(other)).receive(5000)), "the sync send");
            }
        }
    }

    // A sync send to a full FAIL address fails with ResourceAllocationException.
    static void t_failSyncSend() throws Exception {
        String q = unique("ow.fail.sync");
        try (Connection c = cf().createConnection()) {
            c.start();
            int sent = 0;
            JMSException failed = null;
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setDeliveryMode(DeliveryMode.PERSISTENT);
                for (int i = 0; i < 300 && failed == null; i++) {
                    TextMessage m = s.createTextMessage(i + "-" + PAD);
                    m.setIntProperty("i", i);
                    try {
                        p.send(m);
                        sent++;
                    } catch (JMSException e) {
                        failed = e;
                    }
                }
            }
            check(failed instanceof ResourceAllocationException, "a send failed: " + failed);
            eq("Queue is full " + q, failed.getMessage(), "the failure");
            check(sent > 10 && sent < 300, "sends before the address was full: " + sent);
            takeInOrder(c, q, sent);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(s.createTextMessage("again"));
                eq("again", text(s.createConsumer(s.createQueue(q)).receive(5000)), "sent after draining");
            }
        }
    }
}
