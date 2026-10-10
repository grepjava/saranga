// Phase 2 interop suite: JMS (Core) behaviour that Saranga must share with Artemis.
//   java -cp '<artemis>/lib/*' Phase2.java tcp://host:port [test-name...]
// Each test uses its own queues, so the suite runs against a fresh or a used broker.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import java.util.concurrent.*;
import javax.jms.Queue;
import java.util.function.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Phase2 {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("queueSendReceive", Phase2::queueSendReceive);
        tests.put("messageTypes", Phase2::messageTypes);
        tests.put("properties", Phase2::properties);
        tests.put("priorityOrder", Phase2::priorityOrder);
        tests.put("topicNonDurable", Phase2::topicNonDurable);
        tests.put("durableSubscription", Phase2::durableSubscription);
        tests.put("durableSubscriptionSelector", Phase2::durableSubscriptionSelector);
        tests.put("queueSelector", Phase2::queueSelector);
        tests.put("sharedSubscription", Phase2::sharedSubscription);
        tests.put("transacted", Phase2::transacted);
        tests.put("transactedRedelivery", Phase2::transactedRedelivery);
        tests.put("clientAckRecover", Phase2::clientAckRecover);
        tests.put("clientAckCloseRedelivers", Phase2::clientAckCloseRedelivers);
        tests.put("individualAck", Phase2::individualAck);
        tests.put("dupsOk", Phase2::dupsOk);
        tests.put("browser", Phase2::browser);
        tests.put("tempQueueRequestReply", Phase2::tempQueueRequestReply);
        tests.put("roundRobin", Phase2::roundRobin);
        tests.put("receiveNoWait", Phase2::receiveNoWait);
        tests.put("slowConsumer", Phase2::slowConsumer);
        tests.put("listener", Phase2::listener);
        tests.put("fqqn", Phase2::fqqn);
        tests.put("jms2Context", Phase2::jms2Context);
        tests.put("manyMessages", Phase2::manyMessages);

        int failed = 0;
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            if (!only.isEmpty() && !only.contains(t.getKey())) continue;
            long start = System.nanoTime();
            try {
                t.getValue().run();
                System.out.printf("PASS %-30s %5d ms%n", t.getKey(), (System.nanoTime() - start) / 1_000_000);
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-30s %s%n", t.getKey(), e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ----------------------------------------------------------------- helpers

    static String name(String base) {
        return "p2." + base + "." + RUN;
    }

    static ConnectionFactory cf(String params) {
        return new ActiveMQConnectionFactory(params == null ? url : url + "?" + params);
    }

    static void with(String params, ThrowingConsumer<Connection> body) throws Exception {
        try (Connection c = cf(params).createConnection()) {
            body.accept(c);
        }
    }

    static void with(ThrowingConsumer<Connection> body) throws Exception {
        with(null, body);
    }

    interface ThrowingConsumer<T> { void accept(T t) throws Exception; }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static <T> void eq(T expected, T actual, String what) {
        if (!Objects.equals(expected, actual)) throw new AssertionError(what + ": expected " + expected + ", got " + actual);
    }

    static String text(Message m) throws JMSException {
        check(m != null, "no message");
        return ((TextMessage) m).getText();
    }

    static List<String> drain(MessageConsumer c, long firstWait) throws JMSException {
        List<String> out = new ArrayList<>();
        Message m = c.receive(firstWait);
        while (m != null) {
            out.add(text(m));
            m = c.receive(200);
        }
        return out;
    }

    // ------------------------------------------------------------------- tests

    static void queueSendReceive() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("q"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 10; i++) p.send(s.createTextMessage("m" + i));
            MessageConsumer mc = s.createConsumer(q);
            for (int i = 0; i < 10; i++) eq("m" + i, text(mc.receive(5000)), "message " + i);
            check(mc.receive(200) == null, "queue should be empty");
        });
    }

    static void messageTypes() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("types"));
            MessageProducer p = s.createProducer(q);
            p.send(s.createTextMessage("text"));
            BytesMessage b = s.createBytesMessage();
            b.writeBytes(new byte[] {1, 2, 3});
            b.writeInt(42);
            p.send(b);
            MapMessage m = s.createMapMessage();
            m.setString("s", "v");
            m.setDouble("d", 1.5);
            p.send(m);
            StreamMessage st = s.createStreamMessage();
            st.writeString("x");
            st.writeBoolean(true);
            p.send(st);
            p.send(s.createObjectMessage(new java.util.ArrayList<>(List.of("a", "b"))));
            p.send(s.createMessage());

            MessageConsumer mc = s.createConsumer(q);
            eq("text", ((TextMessage) mc.receive(5000)).getText(), "text");
            BytesMessage rb = (BytesMessage) mc.receive(5000);
            byte[] three = new byte[3];
            rb.readBytes(three);
            check(Arrays.equals(three, new byte[] {1, 2, 3}), "bytes body");
            eq(42, rb.readInt(), "bytes int");
            MapMessage rm = (MapMessage) mc.receive(5000);
            eq("v", rm.getString("s"), "map string");
            eq(1.5, rm.getDouble("d"), "map double");
            StreamMessage rs = (StreamMessage) mc.receive(5000);
            eq("x", rs.readString(), "stream string");
            eq(true, rs.readBoolean(), "stream boolean");
            ObjectMessage ro = (ObjectMessage) mc.receive(5000);
            eq(List.of("a", "b"), ro.getObject(), "object");
            Message plain = mc.receive(5000);
            check(plain != null && !(plain instanceof TextMessage), "plain message");
        });
    }

    static void properties() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("props"));
            TextMessage t = s.createTextMessage("p");
            t.setBooleanProperty("b", true);
            t.setByteProperty("y", (byte) -3);
            t.setShortProperty("h", (short) 300);
            t.setIntProperty("i", 70000);
            t.setLongProperty("l", 1L << 40);
            t.setFloatProperty("f", 2.5f);
            t.setDoubleProperty("d", Math.PI);
            t.setStringProperty("s", "héllo ☃");
            t.setJMSCorrelationID("corr-1");
            t.setJMSType("kind");
            s.createProducer(q).send(t, DeliveryMode.PERSISTENT, 7, 0);
            Message r = s.createConsumer(q).receive(5000);
            check(r != null, "received");
            eq(true, r.getBooleanProperty("b"), "bool");
            eq((byte) -3, r.getByteProperty("y"), "byte");
            eq((short) 300, r.getShortProperty("h"), "short");
            eq(70000, r.getIntProperty("i"), "int");
            eq(1L << 40, r.getLongProperty("l"), "long");
            eq(2.5f, r.getFloatProperty("f"), "float");
            eq(Math.PI, r.getDoubleProperty("d"), "double");
            eq("héllo ☃", r.getStringProperty("s"), "string");
            eq("corr-1", r.getJMSCorrelationID(), "correlation id");
            eq("kind", r.getJMSType(), "type");
            eq(7, r.getJMSPriority(), "priority");
            eq(DeliveryMode.PERSISTENT, r.getJMSDeliveryMode(), "delivery mode");
            eq(t.getJMSMessageID(), r.getJMSMessageID(), "message id");
            eq(false, r.getJMSRedelivered(), "redelivered");
            eq(1, r.getIntProperty("JMSXDeliveryCount"), "delivery count");
        });
    }

    static void priorityOrder() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("prio"));
            MessageProducer p = s.createProducer(q);
            int[] prios = {4, 9, 0, 9, 4, 1};
            for (int i = 0; i < prios.length; i++) p.send(s.createTextMessage(prios[i] + "-" + i), DeliveryMode.PERSISTENT, prios[i], 0);
            List<String> got = drain(s.createConsumer(q), 5000);
            eq(List.of("9-1", "9-3", "4-0", "4-4", "1-5", "0-2"), got, "priority order");
        });
    }

    static void topicNonDurable() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = s.createTopic(name("topic"));
            MessageProducer p = s.createProducer(t);
            p.send(s.createTextMessage("before")); // nobody subscribed: dropped
            MessageConsumer a = s.createConsumer(t);
            MessageConsumer b = s.createConsumer(t);
            for (int i = 0; i < 3; i++) p.send(s.createTextMessage("t" + i));
            eq(List.of("t0", "t1", "t2"), drain(a, 5000), "subscriber a");
            eq(List.of("t0", "t1", "t2"), drain(b, 5000), "subscriber b");
        });
    }

    static void durableSubscription() throws Exception {
        String topic = name("dtopic");
        with(c -> {
            c.setClientID("p2-durable-" + RUN);
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = s.createTopic(topic);
            s.createDurableSubscriber(t, "sub").close();
            MessageProducer p = s.createProducer(t);
            p.send(s.createTextMessage("while-away-1"));
            p.send(s.createTextMessage("while-away-2"));
        });
        with(c -> {
            c.setClientID("p2-durable-" + RUN);
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = s.createTopic(topic);
            TopicSubscriber sub = s.createDurableSubscriber(t, "sub");
            eq(List.of("while-away-1", "while-away-2"), drain(sub, 5000), "durable backlog");
            sub.close();
            s.unsubscribe("sub");
        });
    }

    static void durableSubscriptionSelector() throws Exception {
        with(c -> {
            c.setClientID("p2-dsel-" + RUN);
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = s.createTopic(name("dseltopic"));
            TopicSubscriber sub = s.createDurableSubscriber(t, "red", "colour = 'red'", false);
            MessageProducer p = s.createProducer(t);
            for (String colour : List.of("red", "blue", "red")) {
                TextMessage m = s.createTextMessage(colour);
                m.setStringProperty("colour", colour);
                p.send(m);
            }
            eq(List.of("red", "red"), drain(sub, 5000), "selected");
            sub.close();
            s.unsubscribe("red");
        });
    }

    static void queueSelector() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("sel"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 6; i++) {
                TextMessage m = s.createTextMessage("n" + i);
                m.setIntProperty("n", i);
                p.send(m);
            }
            eq(List.of("n1", "n3", "n5"), drain(s.createConsumer(q, "n = 1 OR n = 3 OR n = 5"), 5000), "odd selector");
            eq(List.of("n0", "n2", "n4"), drain(s.createConsumer(q), 5000), "the rest");
        });
    }

    static void sharedSubscription() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = s.createTopic(name("shared"));
            MessageConsumer a = s.createSharedConsumer(t, "group");
            MessageConsumer b = s.createSharedConsumer(t, "group");
            MessageProducer p = s.createProducer(t);
            for (int i = 0; i < 10; i++) p.send(s.createTextMessage("s" + i));
            List<String> all = new ArrayList<>(drain(a, 5000));
            all.addAll(drain(b, 1000));
            Collections.sort(all);
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) expected.add("s" + i);
            Collections.sort(expected);
            eq(expected, all, "shared subscription delivers each message once");
        });
    }

    static void transacted() throws Exception {
        with(c -> {
            c.start();
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            Session plain = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = tx.createQueue(name("tx"));
            MessageProducer p = tx.createProducer(q);
            MessageConsumer reader = plain.createConsumer(q);
            p.send(tx.createTextMessage("a"));
            check(reader.receive(300) == null, "uncommitted send is invisible");
            tx.commit();
            eq("a", text(reader.receive(5000)), "committed send");
            p.send(tx.createTextMessage("b"));
            tx.rollback();
            check(reader.receive(300) == null, "rolled back send is gone");
        });
    }

    static void transactedRedelivery() throws Exception {
        with(c -> {
            c.start();
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = tx.createQueue(name("txr"));
            tx.createProducer(q).send(tx.createTextMessage("r"));
            tx.commit();
            MessageConsumer mc = tx.createConsumer(q);
            Message first = mc.receive(5000);
            eq("r", text(first), "first delivery");
            eq(false, first.getJMSRedelivered(), "first not redelivered");
            tx.rollback();
            Message again = mc.receive(5000);
            eq("r", text(again), "redelivery");
            eq(true, again.getJMSRedelivered(), "redelivered flag");
            eq(2, again.getIntProperty("JMSXDeliveryCount"), "delivery count");
            tx.commit();
            check(mc.receive(300) == null, "consumed after commit");
        });
    }

    static void clientAckRecover() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(name("cack"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 3; i++) p.send(s.createTextMessage("c" + i));
            MessageConsumer mc = s.createConsumer(q);
            for (int i = 0; i < 3; i++) eq("c" + i, text(mc.receive(5000)), "first pass " + i);
            s.recover();
            Message last = null;
            for (int i = 0; i < 3; i++) {
                last = mc.receive(5000);
                eq("c" + i, text(last), "after recover " + i);
                eq(true, last.getJMSRedelivered(), "redelivered after recover");
            }
            last.acknowledge();
            check(mc.receive(300) == null, "all acknowledged");
        });
    }

    static void clientAckCloseRedelivers() throws Exception {
        String queue = name("cackclose");
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(queue);
            MessageProducer p = s.createProducer(q);
            p.send(s.createTextMessage("x"));
            p.send(s.createTextMessage("y"));
            MessageConsumer mc = s.createConsumer(q);
            eq("x", text(mc.receive(5000)), "x");
            eq("y", text(mc.receive(5000)), "y");
            // closed without acknowledging
        });
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            eq(List.of("x", "y"), drain(s.createConsumer(s.createQueue(queue)), 5000), "unacknowledged come back");
        });
    }

    static void individualAck() throws Exception {
        String queue = name("iack");
        with(c -> {
            c.start();
            Session s = c.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
            Queue q = s.createQueue(queue);
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 3; i++) p.send(s.createTextMessage("i" + i));
            MessageConsumer mc = s.createConsumer(q);
            mc.receive(5000);
            Message second = mc.receive(5000);
            mc.receive(5000);
            second.acknowledge();
        });
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            eq(List.of("i0", "i2"), drain(s.createConsumer(s.createQueue(queue)), 5000), "only i1 acknowledged");
        });
    }

    static void dupsOk() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue q = s.createQueue(name("dups"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 20; i++) p.send(s.createTextMessage("d" + i));
            eq(20, drain(s.createConsumer(q), 5000).size(), "dups-ok receives everything");
        });
    }

    static void browser() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("browse"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 4; i++) p.send(s.createTextMessage("b" + i));
            List<String> seen = new ArrayList<>();
            QueueBrowser br = s.createBrowser(q);
            for (Enumeration<?> e = br.getEnumeration(); e.hasMoreElements(); ) seen.add(((TextMessage) e.nextElement()).getText());
            br.close();
            eq(List.of("b0", "b1", "b2", "b3"), seen, "browsed");
            eq(List.of("b0", "b1", "b2", "b3"), drain(s.createConsumer(q), 5000), "still there after browsing");
        });
    }

    static void tempQueueRequestReply() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue service = s.createQueue(name("service"));
            MessageConsumer server = s.createConsumer(service);
            server.setMessageListener(m -> {
                try {
                    Session rs = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    TextMessage reply = rs.createTextMessage("re:" + ((TextMessage) m).getText());
                    reply.setJMSCorrelationID(m.getJMSMessageID());
                    rs.createProducer(m.getJMSReplyTo()).send(reply);
                    rs.close();
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            TemporaryQueue replies = s.createTemporaryQueue();
            TextMessage req = s.createTextMessage("ping");
            req.setJMSReplyTo(replies);
            s.createProducer(service).send(req);
            Message reply = s.createConsumer(replies).receive(5000);
            eq("re:ping", text(reply), "reply");
            eq(req.getJMSMessageID(), reply.getJMSCorrelationID(), "correlation");
        });
    }

    static void roundRobin() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("rr"));
            // Window 0 so neither consumer buffers ahead of the other.
            MessageConsumer a = s.createConsumer(q);
            MessageConsumer b = s.createConsumer(q);
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 10; i++) p.send(s.createTextMessage("r" + i));
            List<String> fromA = drain(a, 5000);
            List<String> fromB = drain(b, 1000);
            eq(10, fromA.size() + fromB.size(), "every message once");
            eq(5, fromA.size(), "consumer a got half");
            eq(List.of("r0", "r2", "r4", "r6", "r8"), fromA, "a alternates");
        });
    }

    static void receiveNoWait() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("nowait"));
            MessageConsumer mc = s.createConsumer(q);
            check(mc.receiveNoWait() == null, "empty queue");
            s.createProducer(q).send(s.createTextMessage("now"));
            Message m = null;
            for (int i = 0; i < 50 && m == null; i++) {
                m = mc.receiveNoWait();
                if (m == null) Thread.sleep(20);
            }
            eq("now", text(m), "receiveNoWait gets the message");
        });
    }

    static void slowConsumer() throws Exception {
        with("consumerWindowSize=0", c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("slow"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 5; i++) p.send(s.createTextMessage("w" + i));
            MessageConsumer a = s.createConsumer(q);
            MessageConsumer b = s.createConsumer(q);
            eq("w0", text(a.receive(5000)), "a takes one");
            eq("w1", text(b.receive(5000)), "b takes the next, nothing was buffered by a");
            eq("w2", text(a.receive(5000)), "a again");
        });
    }

    static void listener() throws Exception {
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("listen"));
            CountDownLatch latch = new CountDownLatch(5);
            List<String> got = Collections.synchronizedList(new ArrayList<>());
            s.createConsumer(q).setMessageListener(m -> {
                try {
                    got.add(((TextMessage) m).getText());
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
            c.start();
            Session ps = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = ps.createProducer(q);
            for (int i = 0; i < 5; i++) p.send(ps.createTextMessage("L" + i));
            check(latch.await(5, TimeUnit.SECONDS), "listener got all");
            eq(List.of("L0", "L1", "L2", "L3", "L4"), got, "listener order");
        });
    }

    static void fqqn() throws Exception {
        String address = name("fqqn");
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue one = s.createQueue(address + "::one");
            Queue two = s.createQueue(address + "::two");
            MessageConsumer c1 = s.createConsumer(one);
            MessageConsumer c2 = s.createConsumer(two);
            s.createProducer(s.createQueue(address + "::one")).send(s.createTextMessage("to-one"));
            eq("to-one", text(c1.receive(5000)), "fqqn send reaches its queue");
            check(c2.receive(300) == null, "and only that queue");
        });
    }

    static void jms2Context() throws Exception {
        try (JMSContext ctx = ((ActiveMQConnectionFactory) cf(null)).createContext()) {
            Queue q = ctx.createQueue(name("ctx"));
            ctx.createProducer().send(q, "jms2");
            eq("jms2", ctx.createConsumer(q).receiveBody(String.class, 5000), "JMSContext round trip");
        }
    }

    static void manyMessages() throws Exception {
        with(c -> {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue(name("many"));
            MessageProducer p = s.createProducer(q);
            p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            int n = 20_000;
            byte[] payload = new byte[512];
            for (int i = 0; i < n; i++) {
                BytesMessage m = s.createBytesMessage();
                m.writeBytes(payload);
                m.setIntProperty("i", i);
                p.send(m);
            }
            MessageConsumer mc = s.createConsumer(q);
            for (int i = 0; i < n; i++) {
                Message m = mc.receive(5000);
                check(m != null, "message " + i + " of " + n);
                eq(i, m.getIntProperty("i"), "order");
            }
        });
    }
}
