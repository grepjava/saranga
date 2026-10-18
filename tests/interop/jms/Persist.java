// Persistence interop suite: what a broker must recover after a restart.
//   java -cp '<artemis>/lib/*' Persist.java tcp://host:port before|after <run-id> [test-name...]
// scripts/persistence.sh runs "before", stops (or kills) the broker, starts the
// same broker or the other one on the same data directory, and runs "after".
// Run it against Artemis on both sides too: a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

public class Persist {
    static String url;
    static String run;

    interface Step { void run() throws Exception; }

    record Test(Step before, Step after) {}

    public static void main(String[] args) throws Exception {
        url = args[0];
        String phase = args[1];
        run = args[2];
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(3, args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("durableQueue", new Test(Persist::durableQueueBefore, Persist::durableQueueAfter));
        tests.put("ackedNotRedelivered", new Test(Persist::ackedBefore, Persist::ackedAfter));
        tests.put("durableSubscription", new Test(Persist::durableSubBefore, Persist::durableSubAfter));
        tests.put("transactions", new Test(Persist::txBefore, Persist::txAfter));
        tests.put("deliveryCount", new Test(Persist::deliveryCountBefore, Persist::deliveryCountAfter));
        tests.put("xaPrepared", new Test(Persist::xaBefore, Persist::xaAfter));
        tests.put("queueAttributes", new Test(Persist::attributesBefore, Persist::attributesAfter));
        tests.put("manyMessages", new Test(Persist::manyBefore, Persist::manyAfter));
        tests.put("largeMessages", new Test(Persist::largeBefore, Persist::largeAfter));
        tests.put("largeXaPrepared", new Test(Persist::largeXaBefore, Persist::largeXaAfter));
        tests.put("paging", new Test(Persist::pagingBefore, Persist::pagingAfter));
        tests.put("pagedXaPrepared", new Test(Persist::pagedXaBefore, Persist::pagedXaAfter));
        tests.put("amqpMessages", new Test(Persist::amqpBefore, Persist::amqpAfter));
        tests.put("amqpDurableSubscription", new Test(Persist::amqpSubBefore, Persist::amqpSubAfter));

        int failed = 0;
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            if (!only.isEmpty() && !only.contains(t.getKey())) continue;
            long start = System.nanoTime();
            try {
                (phase.equals("before") ? t.getValue().before() : t.getValue().after()).run();
                System.out.printf("PASS %-6s %-24s %5d ms%n", phase, t.getKey(), (System.nanoTime() - start) / 1_000_000);
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-6s %-24s %s%n", phase, t.getKey(), e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ----------------------------------------------------------------- helpers

    static String name(String base) {
        return "persist." + base + "." + run;
    }

    static Xid xid(int n) {
        return new XidImpl(("bq" + n).getBytes(), 7, ("persist-" + run + "-" + n).getBytes());
    }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    interface Body { void run(Session s) throws Exception; }

    static void session(boolean transacted, int ack, Body body) throws Exception {
        try (Connection c = new ActiveMQConnectionFactory(url).createConnection()) {
            c.setClientID("persist-" + run);
            c.start();
            try (Session s = c.createSession(transacted, ack)) {
                body.run(s);
            }
        }
    }

    static void session(Body body) throws Exception {
        session(false, Session.AUTO_ACKNOWLEDGE, body);
    }

    static List<String> drain(Session s, Destination d) throws Exception {
        List<String> got = new ArrayList<>();
        MessageConsumer mc = s.createConsumer(d);
        Message m;
        while ((m = mc.receive(1000)) != null) got.add(((TextMessage) m).getText());
        mc.close();
        return got;
    }

    static void sendText(Session s, Destination d, int mode, String... texts) throws Exception {
        MessageProducer p = s.createProducer(d);
        p.setDeliveryMode(mode);
        for (String t : texts) p.send(s.createTextMessage(t));
    }

    /** A session of an AMQP (Qpid JMS) connection to the same broker. */
    static void amqpSession(String clientId, Body body) throws Exception {
        java.net.URI u = new java.net.URI(url);
        String amqp = "amqp://" + u.getHost() + ":" + u.getPort();
        try (Connection c = new org.apache.qpid.jms.JmsConnectionFactory(amqp).createConnection()) {
            if (clientId != null) c.setClientID(clientId);
            c.start();
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                body.run(s);
            }
        }
    }

    static byte[] largeAmqpBody() {
        byte[] b = new byte[300 * 1024];
        new Random(42).nextBytes(b);
        return b;
    }

    // ------------------------------------------------------------------- tests

    /**
     * AMQP messages are kept in their own encoding: small ones in journal
     * records, a large one (over amqpMinLargeMessageSize) in a file. One is
     * acknowledged before the restart.
     */
    static void amqpBefore() throws Exception {
        amqpSession(null, s -> {
            Queue q = s.createQueue(name("amqp"));
            MessageProducer p = s.createProducer(q);
            p.setDeliveryMode(DeliveryMode.PERSISTENT);
            // Highest priority, so it is the one consumed first.
            p.send(s.createTextMessage("acked"), DeliveryMode.PERSISTENT, 9, 0);
            TextMessage t = s.createTextMessage("amqp text");
            t.setStringProperty("s", "v");
            t.setIntProperty("i", 7);
            t.setJMSCorrelationID("corr");
            p.send(t, DeliveryMode.PERSISTENT, 6, 0);
            MapMessage m = s.createMapMessage();
            m.setString("k", "v");
            p.send(m);
            BytesMessage large = s.createBytesMessage();
            large.writeBytes(largeAmqpBody());
            p.send(large);
            MessageConsumer c = s.createConsumer(q);
            Message first = c.receive(5000);
            check(first instanceof TextMessage && "acked".equals(((TextMessage) first).getText()), "first: " + first);
            c.close();
        });
    }

    static void amqpAfter() throws Exception {
        amqpSession(null, s -> {
            MessageConsumer c = s.createConsumer(s.createQueue(name("amqp")));
            Message t = c.receive(5000);
            check(t instanceof TextMessage && "amqp text".equals(((TextMessage) t).getText()), "text: " + t);
            check("v".equals(t.getStringProperty("s")) && t.getIntProperty("i") == 7, "properties of " + t);
            check("corr".equals(t.getJMSCorrelationID()) && t.getJMSPriority() == 6, "headers of " + t);
            Message m = c.receive(5000);
            check(m instanceof MapMessage && "v".equals(((MapMessage) m).getString("k")), "map: " + m);
            Message l = c.receive(5000);
            check(l instanceof BytesMessage, "large: " + l);
            byte[] got = new byte[(int) ((BytesMessage) l).getBodyLength()];
            ((BytesMessage) l).readBytes(got);
            check(Arrays.equals(largeAmqpBody(), got), "large body of " + got.length + " bytes");
            check(c.receive(500) == null, "nothing more, the acked message included");
        });
    }

    /** An AMQP durable subscription and its message, read back as Core. */
    static void amqpSubBefore() throws Exception {
        amqpSession("amqp-persist-" + run, s -> {
            Topic t = s.createTopic(name("amqptopic"));
            s.createDurableSubscriber(t, "sub").close();
            s.createProducer(t).send(s.createTextMessage("for the subscription"));
        });
    }

    static void amqpSubAfter() throws Exception {
        session(s -> {
            String fqqn = name("amqptopic") + "::" + "amqp-persist-" + run + ".sub";
            List<String> got = drain(s, s.createQueue(fqqn));
            check(got.equals(List.of("for the subscription")), "subscription queue: " + got);
        });
        amqpSession("amqp-persist-" + run, s -> s.unsubscribe("sub"));
    }

    /** Persistent messages survive with their properties and order; non-persistent ones do not. */
    static void durableQueueBefore() throws Exception {
        session(s -> {
            Queue q = s.createQueue(name("dq"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 3; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setIntProperty("n", i);
                m.setStringProperty("s", "ünicode-" + i);
                m.setJMSCorrelationID("corr-" + i);
                p.send(m, DeliveryMode.PERSISTENT, i == 2 ? 9 : 4, 0);
            }
            p.send(s.createTextMessage("transient"), DeliveryMode.NON_PERSISTENT, 4, 0);
            BytesMessage b = s.createBytesMessage();
            b.writeBytes(new byte[]{1, 2, 3, (byte) 0xff});
            p.send(b);
        });
    }

    static void durableQueueAfter() throws Exception {
        session(s -> {
            MessageConsumer mc = s.createConsumer(s.createQueue(name("dq")));
            List<String> order = new ArrayList<>();
            Message m;
            while ((m = mc.receive(1000)) != null) {
                if (m instanceof TextMessage t) {
                    String text = t.getText();
                    order.add(text);
                    int n = t.getIntProperty("n");
                    check(text.equals("m" + n), "body matches property");
                    check(t.getStringProperty("s").equals("ünicode-" + n), "string property");
                    check(t.getJMSCorrelationID().equals("corr-" + n), "correlation id");
                    check(t.getJMSDeliveryMode() == DeliveryMode.PERSISTENT, "persistent");
                } else {
                    BytesMessage b = (BytesMessage) m;
                    byte[] data = new byte[(int) b.getBodyLength()];
                    b.readBytes(data);
                    check(Arrays.equals(data, new byte[]{1, 2, 3, (byte) 0xff}), "bytes body");
                    order.add("bytes");
                }
            }
            // Priority 9 first, then FIFO.
            check(order.equals(List.of("m2", "m0", "m1", "bytes")), "recovered " + order);
        });
    }

    /** Acknowledged messages stay acknowledged. */
    static void ackedBefore() throws Exception {
        session(s -> {
            Queue q = s.createQueue(name("acked"));
            sendText(s, q, DeliveryMode.PERSISTENT, "a", "b", "c", "d", "e");
        });
        session(false, Session.CLIENT_ACKNOWLEDGE, s -> {
            MessageConsumer mc = s.createConsumer(s.createQueue(name("acked")));
            mc.receive(2000);
            Message second = mc.receive(2000);
            second.acknowledge();
        });
    }

    static void ackedAfter() throws Exception {
        session(s -> check(drain(s, s.createQueue(name("acked"))).equals(List.of("c", "d", "e")),
            "only the unacknowledged messages"));
    }

    /** A durable subscription and its messages survive; a non-durable one does not. */
    static void durableSubBefore() throws Exception {
        session(s -> {
            Topic t = s.createTopic(name("topic"));
            s.createDurableSubscriber(t, "sub").close();
            s.createDurableSubscriber(t, "red", "color = 'red'", false).close();
            MessageProducer p = s.createProducer(t);
            for (String color : new String[]{"red", "blue", "red"}) {
                TextMessage m = s.createTextMessage(color);
                m.setStringProperty("color", color);
                p.send(m);
            }
        });
    }

    static void durableSubAfter() throws Exception {
        session(s -> {
            Topic t = s.createTopic(name("topic"));
            List<String> all = new ArrayList<>();
            MessageConsumer c = s.createDurableSubscriber(t, "sub");
            Message m;
            while ((m = c.receive(1000)) != null) all.add(((TextMessage) m).getText());
            c.close();
            check(all.equals(List.of("red", "blue", "red")), "durable subscription " + all);
            List<String> red = new ArrayList<>();
            c = s.createDurableSubscriber(t, "red", "color = 'red'", false);
            while ((m = c.receive(1000)) != null) red.add(((TextMessage) m).getText());
            c.close();
            check(red.equals(List.of("red", "red")), "selector subscription " + red);
        });
    }

    /** Committed transactional work survives; rolled-back work does not. */
    static void txBefore() throws Exception {
        session(s -> sendText(s, s.createQueue(name("tx.src")), DeliveryMode.PERSISTENT, "x", "y"));
        session(true, Session.SESSION_TRANSACTED, s -> {
            Queue src = s.createQueue(name("tx.src"));
            Queue dst = s.createQueue(name("tx.dst"));
            MessageConsumer mc = s.createConsumer(src);
            check(mc.receive(2000) != null, "x");
            sendText(s, dst, DeliveryMode.PERSISTENT, "committed");
            s.commit();
            check(mc.receive(2000) != null, "y");
            sendText(s, dst, DeliveryMode.PERSISTENT, "rolled back");
            s.rollback();
        });
    }

    static void txAfter() throws Exception {
        session(s -> {
            check(drain(s, s.createQueue(name("tx.src"))).equals(List.of("y")), "the committed ack holds");
            check(drain(s, s.createQueue(name("tx.dst"))).equals(List.of("committed")), "only committed sends");
        });
    }

    /** Redelivery counts recorded on rollback are kept. */
    static void deliveryCountBefore() throws Exception {
        session(s -> sendText(s, s.createQueue(name("dc")), DeliveryMode.PERSISTENT, "redelivered"));
        session(true, Session.SESSION_TRANSACTED, s -> {
            MessageConsumer mc = s.createConsumer(s.createQueue(name("dc")));
            for (int i = 0; i < 2; i++) {
                check(mc.receive(2000) != null, "delivery " + i);
                s.rollback();
            }
        });
    }

    static void deliveryCountAfter() throws Exception {
        session(s -> {
            Message m = s.createConsumer(s.createQueue(name("dc"))).receive(2000);
            check(m != null, "message kept");
            // tests/golden/etc/broker.xml sets max-redelivery-records to 1:
            // the first rollback persisted a count of 1, the second nothing,
            // so after the restart this is the second delivery.
            check(m.getIntProperty("JMSXDeliveryCount") == 2, "delivery count " + m.getIntProperty("JMSXDeliveryCount"));
        });
    }

    /** A prepared XA branch survives and can be completed after the restart. */
    static void xaBefore() throws Exception {
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendText(s, s.createQueue(name("xa.acked")), DeliveryMode.PERSISTENT, "acked-in-xa", "left");
            }
            XASession xs = xc.createXASession();
            XAResource xa = xs.getXAResource();
            Xid x = xid(1);
            xa.start(x, XAResource.TMNOFLAGS);
            sendText(xs, xs.createQueue(name("xa.sent")), DeliveryMode.PERSISTENT, "sent-in-xa");
            MessageConsumer mc = xs.createConsumer(xs.createQueue(name("xa.acked")));
            check(mc.receive(2000) != null, "received in xa");
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
            Xid y = xid(2);
            xa.start(y, XAResource.TMNOFLAGS);
            sendText(xs, xs.createQueue(name("xa.sent")), DeliveryMode.PERSISTENT, "prepared-then-rolled-back");
            xa.end(y, XAResource.TMSUCCESS);
            xa.prepare(y);
        }
    }

    static void xaAfter() throws Exception {
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            XAResource xa = xc.createXASession().getXAResource();
            List<Xid> inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(inDoubt.contains(xid(1)) && inDoubt.contains(xid(2)), "both branches in doubt: " + inDoubt);
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                check(drain(s, s.createQueue(name("xa.sent"))).isEmpty(), "prepared sends not delivered");
                check(drain(s, s.createQueue(name("xa.acked"))).equals(List.of("left")), "prepared ack holds its message");
            }
            xa.commit(xid(1), false);
            xa.rollback(xid(2));
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                check(drain(s, s.createQueue(name("xa.sent"))).equals(List.of("sent-in-xa")), "committed send delivered");
                check(drain(s, s.createQueue(name("xa.acked"))).isEmpty(), "committed ack removed the message");
            }
        }
    }

    /** Queue attributes set at creation are recovered. */
    static void attributesBefore() throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession cs = sf.createSession()) {
            cs.createQueue(QueueConfiguration.of(name("attrs"))
                .setRoutingType(RoutingType.ANYCAST)
                .setFilterString("x > 1")
                .setMaxConsumers(3)
                .setExclusive(true)
                .setLastValue(true)
                .setLastValueKey("k")
                .setRingSize(20L)
                .setDurable(true));
        }
    }

    static void attributesAfter() throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession cs = sf.createSession()) {
            ClientSession.QueueQuery q = cs.queueQuery(org.apache.activemq.artemis.api.core.SimpleString.of(name("attrs")));
            check(q.isExists(), "queue recovered");
            check(q.getRoutingType() == RoutingType.ANYCAST, "routing type");
            check(q.getFilterString().toString().equals("x > 1"), "filter");
            check(q.getMaxConsumers() == 3, "max consumers");
            check(q.isExclusive(), "exclusive");
            check(q.isLastValue() && q.getLastValueKey().toString().equals("k"), "last value");
            check(q.getRingSize() == 20L, "ring size");
            check(q.isDurable(), "durable");
        }
    }

    /** Enough messages to span journal files, half of them consumed. */
    static void manyBefore() throws Exception {
        session(s -> {
            Queue q = s.createQueue(name("many"));
            MessageProducer p = s.createProducer(q);
            p.setDeliveryMode(DeliveryMode.PERSISTENT);
            char[] pad = new char[2000];
            Arrays.fill(pad, 'x');
            for (int i = 0; i < 4000; i++) {
                TextMessage m = s.createTextMessage(new String(pad));
                m.setIntProperty("i", i);
                p.send(m);
            }
            MessageConsumer mc = s.createConsumer(q);
            for (int i = 0; i < 2000; i++) {
                check(mc.receive(5000).getIntProperty("i") == i, "consumed in order");
            }
        });
    }

    static byte[] pattern(int length, int seed) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) b[i] = (byte) (i * 31 + seed * 7 + (i >> 13));
        return b;
    }

    static BytesMessage bytes(Session s, byte[] body, int n) throws JMSException {
        BytesMessage m = s.createBytesMessage();
        m.writeBytes(body);
        m.setIntProperty("n", n);
        return m;
    }

    /** Receives and checks large messages {@code ns}, in order, then nothing more. */
    static void receiveLarge(Session s, Destination d, int... ns) throws Exception {
        MessageConsumer mc = s.createConsumer(d);
        for (int n : ns) {
            BytesMessage m = (BytesMessage) mc.receive(5000);
            check(m != null, "large message " + n);
            check(m.getIntProperty("n") == n, "large message " + n + " in order, got " + m.getIntProperty("n"));
            byte[] expected = pattern(300_000 * n + n, n);
            byte[] got = new byte[(int) m.getBodyLength()];
            m.readBytes(got);
            check(Arrays.equals(expected, got), "large message " + n + " body");
        }
        check(mc.receive(500) == null, "nothing more on " + d);
        mc.close();
    }

    /**
     * Large messages: their bodies are files beside the journal. Covers an acked one (its
     * file goes), one delivered but not acked, one on two durable subscriptions, and a
     * compressed one.
     */
    static void largeBefore() throws Exception {
        session(s -> {
            Queue q = s.createQueue(name("large"));
            MessageProducer p = s.createProducer(q);
            for (int n = 1; n <= 4; n++) p.send(bytes(s, pattern(300_000 * n + n, n), n));
            MessageConsumer mc = s.createConsumer(q);
            check(mc.receive(5000).getIntProperty("n") == 1, "first large message consumed");
            mc.close();
            // Topic with two durable subscriptions sharing one message.
            Topic t = s.createTopic(name("large.topic"));
            s.createDurableSubscriber(t, "one").close();
            s.createDurableSubscriber(t, "two").close();
            s.createProducer(t).send(bytes(s, pattern(300_000 * 5 + 5, 5), 5));
        });
        session(false, Session.CLIENT_ACKNOWLEDGE, s -> {
            // Delivered but never acknowledged.
            MessageConsumer mc = s.createConsumer(s.createQueue(name("large")));
            Message m = mc.receive(5000);
            check(m != null && m.getIntProperty("n") == 2, "second large message delivered");
        });
        try (Connection c = new ActiveMQConnectionFactory(url + "?compressLargeMessage=true").createConnection()) {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(name("large.zip"))).send(bytes(s, pattern(300_000 * 6 + 6, 6), 6));
            }
        }
    }

    static void largeAfter() throws Exception {
        session(s -> {
            receiveLarge(s, s.createQueue(name("large")), 2, 3, 4);
            Topic t = s.createTopic(name("large.topic"));
            receiveLargeSub(s, t, "one", 5);
            receiveLargeSub(s, t, "two", 5);
            receiveLarge(s, s.createQueue(name("large.zip")), 6);
        });
    }

    static void receiveLargeSub(Session s, Topic t, String sub, int n) throws Exception {
        MessageConsumer mc = s.createDurableSubscriber(t, sub);
        BytesMessage m = (BytesMessage) mc.receive(5000);
        check(m != null && m.getIntProperty("n") == n, "subscription " + sub + " has large message " + n);
        byte[] got = new byte[(int) m.getBodyLength()];
        m.readBytes(got);
        check(Arrays.equals(pattern(300_000 * n + n, n), got), "subscription " + sub + " body");
        check(mc.receive(500) == null, "nothing more on " + sub);
        mc.close();
        s.unsubscribe(sub);
    }

    /** A large message sent in a prepared XA branch survives with the branch. */
    static void largeXaBefore() throws Exception {
        try (XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            xc.start();
            XASession xs = xc.createXASession();
            XAResource xa = xs.getXAResource();
            Xid x = xid(11);
            xa.start(x, XAResource.TMNOFLAGS);
            xs.createProducer(xs.createQueue(name("large.xa"))).send(bytes(xs, pattern(300_000 * 7 + 7, 7), 7));
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
        }
    }

    static void largeXaAfter() throws Exception {
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            XAResource xa = xc.createXASession().getXAResource();
            List<Xid> inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(inDoubt.contains(xid(11)), "large branch in doubt: " + inDoubt);
            xa.commit(xid(11), false);
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                receiveLarge(s, s.createQueue(name("large.xa")), 7);
            }
        }
    }

    /** Names on golden.paging.#, which pages above 100 KiB (persist-paging.broker.xml). */
    static String paged(String base) {
        return "golden.paging." + base + "." + run;
    }

    static final String PAGE_PAD = "q".repeat(1000);

    static void sendPaged(Session s, Destination d, String prefix, int n) throws Exception {
        MessageProducer p = s.createProducer(d);
        p.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < n; i++) {
            TextMessage m = s.createTextMessage(prefix + "-" + i + "-" + PAGE_PAD);
            m.setIntProperty("i", i);
            p.send(m);
        }
        p.close();
    }

    static void expectPaged(MessageConsumer c, String prefix, int from, int to) throws Exception {
        for (int i = from; i < to; i++) {
            Message m = c.receive(5000);
            check(m != null, prefix + " message " + i);
            check((prefix + "-" + i + "-" + PAGE_PAD).equals(((TextMessage) m).getText()), prefix + " message " + i + " in order");
        }
    }

    /**
     * Paged messages: a queue partly consumed across pages, durable subscriptions consumed
     * unevenly, a committed transaction next to a rolled-back one, a prepared XA branch and a
     * large message, all on paging addresses.
     */
    static void pagingBefore() throws Exception {
        session(s -> {
            Queue q = s.createQueue(paged("q"));
            sendPaged(s, q, "q", 600);
            try (MessageConsumer c = s.createConsumer(q)) {
                expectPaged(c, "q", 0, 250);
            }
            Topic t = s.createTopic(paged("topic"));
            s.createDurableSubscriber(t, "pa").close();
            s.createDurableSubscriber(t, "pb").close();
            sendPaged(s, t, "t", 300);
            try (TopicSubscriber a = s.createDurableSubscriber(t, "pa")) {
                expectPaged(a, "t", 0, 100);
            }
        });
        session(true, Session.SESSION_TRANSACTED, s -> {
            Queue q = s.createQueue(paged("tx"));
            sendPaged(s, q, "fill", 150);
            s.commit();
            sendPaged(s, q, "committed", 80);
            s.commit();
            sendPaged(s, q, "rolledback", 60);
            s.rollback();
            MessageProducer p = s.createProducer(q);
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(pattern(300_000 + 8, 8));
            m.setIntProperty("n", 8);
            p.send(m);
            s.commit();
        });
        try (XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            xc.start();
            XASession xs = xc.createXASession();
            Queue q = xs.createQueue(paged("xa"));
            sendPaged(xs, q, "xfill", 150);
            XAResource xa = xs.getXAResource();
            Xid x = xid(21);
            xa.start(x, XAResource.TMNOFLAGS);
            sendPaged(xs, q, "prepared", 40);
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
        }
    }

    static void pagingAfter() throws Exception {
        session(s -> {
            try (MessageConsumer c = s.createConsumer(s.createQueue(paged("q")))) {
                expectPaged(c, "q", 250, 600);
                check(c.receive(500) == null, "nothing more on the queue");
            }
            Topic t = s.createTopic(paged("topic"));
            try (TopicSubscriber a = s.createDurableSubscriber(t, "pa")) {
                expectPaged(a, "t", 100, 300);
                check(a.receive(500) == null, "nothing more for pa");
            }
            try (TopicSubscriber b = s.createDurableSubscriber(t, "pb")) {
                expectPaged(b, "t", 0, 300);
                check(b.receive(500) == null, "nothing more for pb");
            }
            s.unsubscribe("pa");
            s.unsubscribe("pb");
            try (MessageConsumer c = s.createConsumer(s.createQueue(paged("tx")))) {
                expectPaged(c, "fill", 0, 150);
                expectPaged(c, "committed", 0, 80);
                BytesMessage m = (BytesMessage) c.receive(5000);
                check(m != null && m.getIntProperty("n") == 8, "the paged large message");
                byte[] got = new byte[(int) m.getBodyLength()];
                m.readBytes(got);
                check(Arrays.equals(pattern(300_000 + 8, 8), got), "the large body");
                check(c.receive(500) == null, "rolled-back messages never arrive");
            }
        });
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            XAResource xa = xc.createXASession().getXAResource();
            List<Xid> inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(inDoubt.contains(xid(21)), "paged branch in doubt: " + inDoubt);
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                try (MessageConsumer c = s.createConsumer(s.createQueue(paged("xa")))) {
                    expectPaged(c, "xfill", 0, 150);
                    check(c.receive(500) == null, "prepared paged messages wait");
                }
                xa.commit(xid(21), false);
                // Artemis delivers them to the next consumer: nothing prompts the queue
                // when the recovered branch commits.
                try (MessageConsumer c = s.createConsumer(s.createQueue(paged("xa")))) {
                    expectPaged(c, "prepared", 0, 40);
                    check(c.receive(500) == null, "nothing more on xa");
                }
            }
        }
    }

    /**
     * A prepared XA branch that fills a paging address by itself: the address pages during
     * the branch, so its pages hold only the branch's messages. Committed after the restart,
     * every message arrives, those in the pages too.
     */
    static void pagedXaBefore() throws Exception {
        try (XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            xc.start();
            XASession xs = xc.createXASession();
            Queue q = xs.createQueue(paged("xaonly"));
            XAResource xa = xs.getXAResource();
            Xid x = xid(22);
            xa.start(x, XAResource.TMNOFLAGS);
            sendPaged(xs, q, "branch", 80);
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
        }
    }

    static void pagedXaAfter() throws Exception {
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            XAResource xa = xc.createXASession().getXAResource();
            List<Xid> inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(inDoubt.contains(xid(22)), "the branch in doubt: " + inDoubt);
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                try (MessageConsumer c = s.createConsumer(s.createQueue(paged("xaonly")))) {
                    check(c.receive(500) == null, "prepared messages wait");
                }
                xa.commit(xid(22), false);
                try (MessageConsumer c = s.createConsumer(s.createQueue(paged("xaonly")))) {
                    expectPaged(c, "branch", 0, 80);
                    check(c.receive(500) == null, "nothing more on xaonly");
                }
            }
        }
    }

    static void manyAfter() throws Exception {
        session(s -> {
            MessageConsumer mc = s.createConsumer(s.createQueue(name("many")));
            for (int i = 2000; i < 4000; i++) {
                Message m = mc.receive(5000);
                check(m != null && m.getIntProperty("i") == i, "message " + i);
            }
            check(mc.receive(500) == null, "nothing more");
        });
    }
}
