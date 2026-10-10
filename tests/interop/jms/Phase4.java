// Phase 4 interop suite: queue semantics that Saranga must share with Artemis
// (address settings, dead letters, redelivery delay, expiry, scheduled delivery,
// duplicate detection, priority, groups, exclusive / last-value / non-destructive /
// ring queues, dispatch gating, temporary queues, selectors, diverts, flow control).
//   java -cp '<artemis>/lib/*' Phase4.java tcp://host:port [test-name...]
// The broker must run tests/interop/jms/phase4.broker.xml:
//   CONFIG=tests/interop/jms/phase4.broker.xml SUITE=Phase4 scripts/interop.sh [saranga|artemis]
// Each test uses its own queues (p4.<setting-prefix>.<test>.<run>); messages that
// land on shared queues (DLQ, ExpiryQueue, p4.DLA, p4.EXP, divert queues) are
// tagged with a per-run "p4id" property and picked out with a selector.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Phase4 {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        // address settings, dead letters, redelivery
        tests.put("dlaAfterMaxAttempts", Phase4::dlaAfterMaxAttempts);
        tests.put("settingsHierarchy", Phase4::settingsHierarchy);
        tests.put("dlaCatchAll", Phase4::dlaCatchAll);
        tests.put("noDlaDropped", Phase4::noDlaDropped);
        tests.put("autoCreateDeadLetterQueue", Phase4::autoCreateDeadLetterQueue);
        tests.put("redeliveryDelay", Phase4::redeliveryDelay);
        tests.put("redeliveryDelayOthersFlow", Phase4::redeliveryDelayOthersFlow);
        tests.put("redeliveryBackoff", Phase4::redeliveryBackoff);
        // expiry
        tests.put("expiryToExpiryAddress", Phase4::expiryToExpiryAddress);
        tests.put("expiryCatchAll", Phase4::expiryCatchAll);
        tests.put("expiryInConsumerBuffer", Phase4::expiryInConsumerBuffer);
        tests.put("expiryDelay", Phase4::expiryDelay);
        tests.put("noExpiryAddressDropped", Phase4::noExpiryAddressDropped);
        tests.put("autoCreateExpiryQueue", Phase4::autoCreateExpiryQueue);
        // scheduled delivery
        tests.put("scheduledDelivery", Phase4::scheduledDelivery);
        tests.put("deliveryDelayJms2", Phase4::deliveryDelayJms2);
        tests.put("scheduledOrdering", Phase4::scheduledOrdering);
        tests.put("scheduledTransacted", Phase4::scheduledTransacted);
        tests.put("scheduledSurvivesConsumerClose", Phase4::scheduledSurvivesConsumerClose);
        // duplicate detection
        tests.put("duplicateNonTx", Phase4::duplicateNonTx);
        tests.put("duplicateTransacted", Phase4::duplicateTransacted);
        tests.put("duplicateCacheSize", Phase4::duplicateCacheSize);
        // priority, groups
        tests.put("priorityOrdering", Phase4::priorityOrdering);
        tests.put("groupSticky", Phase4::groupSticky);
        tests.put("groupFirstKey", Phase4::groupFirstKey);
        tests.put("groupCloseBySequence", Phase4::groupCloseBySequence);
        tests.put("groupBuckets", Phase4::groupBuckets);
        tests.put("groupRebalance", Phase4::groupRebalance);
        // special queues
        tests.put("exclusiveQueue", Phase4::exclusiveQueue);
        tests.put("exclusiveFailover", Phase4::exclusiveFailover);
        tests.put("lastValueQueue", Phase4::lastValueQueue);
        tests.put("lastValueKey", Phase4::lastValueKey);
        tests.put("lastValueQueueParam", Phase4::lastValueQueueParam);
        tests.put("nonDestructive", Phase4::nonDestructive);
        tests.put("nonDestructiveLastValue", Phase4::nonDestructiveLastValue);
        tests.put("ringQueue", Phase4::ringQueue);
        tests.put("ringQueueCoreConfig", Phase4::ringQueueCoreConfig);
        tests.put("consumersBeforeDispatch", Phase4::consumersBeforeDispatch);
        tests.put("delayBeforeDispatch", Phase4::delayBeforeDispatch);
        // misc
        tests.put("tempQueueDeletedOnClose", Phase4::tempQueueDeletedOnClose);
        tests.put("selectorLeavesOthers", Phase4::selectorLeavesOthers);
        tests.put("divertExclusive", Phase4::divertExclusive);
        tests.put("divertNonExclusive", Phase4::divertNonExclusive);
        tests.put("divertFilter", Phase4::divertFilter);
        tests.put("divertRoutingType", Phase4::divertRoutingType);
        tests.put("consumerWindowSize", Phase4::consumerWindowSize);
        tests.put("sendToDlaOnNoRoute", Phase4::sendToDlaOnNoRoute);
        tests.put("xaTransactionTimeout", Phase4::xaTransactionTimeout);

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

    static void sendToDlaOnNoRoute() throws Exception {
        // p4.noroute.#: send-to-dla-on-no-route, dead-letter-address p4.DLA
        String t = name("noroute.t");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = s.createTopic(t);
                MessageProducer p = s.createProducer(topic);
                p.send(tagged(s, "nobody", t));
                // With a subscriber the message routes normally.
                MessageConsumer sub = s.createConsumer(topic);
                p.send(tagged(s, "somebody", t));
                eq("somebody", text(sub.receive(2000)), "subscriber gets the routed message");
            }
            List<Message> dead = take(c, "p4.DLA", byId(t), 2000);
            eq(List.of("nobody"), texts(dead), "unrouted message on the dead-letter address");
            eq(t, dead.get(0).getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
        });
    }

    static void xaTransactionTimeout() throws Exception {
        // A branch past its timeout is rolled back by the broker; its send never arrives.
        String q = name("plain.xatimeout");
        try (XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection();
             Connection plain = cf(null).createConnection()) {
            xc.start();
            plain.start();
            XASession xs = xc.createXASession();
            XAResource xa = xs.getXAResource();
            check(xa.setTransactionTimeout(1), "timeout set");
            Xid xid = new XidImpl("b".getBytes(), 7, ("p4-" + RUN).getBytes());
            xa.start(xid, XAResource.TMNOFLAGS);
            xs.createProducer(xs.createQueue(q)).send(xs.createTextMessage("late"));
            Thread.sleep(3500);
            List<String> codes = new ArrayList<>();
            try {
                xa.end(xid, XAResource.TMSUCCESS);
                codes.add("end ok");
            } catch (XAException e) {
                codes.add("end " + e.errorCode);
            }
            try {
                xa.commit(xid, true);
                codes.add("commit ok");
            } catch (XAException e) {
                codes.add("commit " + e.errorCode);
            }
            eq(List.of("end -6", "commit -4"), codes, "end and commit after the timeout");
            check(take(plain, q, null, 500).isEmpty(), "the timed-out send never arrives");
        }
    }

    // ----------------------------------------------------------------- helpers

    /** {@code p4.<base>.<run>}: the base starts with the address-settings prefix the test relies on. */
    static String name(String base) {
        return "p4." + base + "." + RUN;
    }

    static ConnectionFactory cf(String params) {
        return new ActiveMQConnectionFactory(params == null ? url : url + "?" + params);
    }

    interface ThrowingConsumer<T> { void accept(T t) throws Exception; }

    static void with(String params, ThrowingConsumer<Connection> body) throws Exception {
        try (Connection c = cf(params).createConnection()) {
            c.start();
            body.accept(c);
        }
    }

    static void with(ThrowingConsumer<Connection> body) throws Exception {
        with(null, body);
    }

    /** A Core API session, for what JMS cannot express (queue attributes, queue queries). */
    static void core(ThrowingConsumer<ClientSession> body) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            s.start();
            body.accept(s);
        }
    }

    static void createQueue(QueueConfiguration qc) throws Exception {
        core(s -> s.createQueue(qc.setRoutingType(RoutingType.ANYCAST).setAutoCreateAddress(true)));
    }

    static ClientSession.QueueQuery query(String queue) throws Exception {
        ClientSession.QueueQuery[] out = new ClientSession.QueueQuery[1];
        core(s -> out[0] = s.queueQuery(SimpleString.of(queue)));
        return out[0];
    }

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

    interface MessageMaker { Message make(Session s) throws JMSException; }

    static TextMessage tagged(Session s, String text, String id) throws JMSException {
        TextMessage m = s.createTextMessage(text);
        m.setStringProperty("p4id", id);
        return m;
    }

    /** Sends each message on its own auto-ack session. */
    static void send(Connection c, String queue, MessageMaker... makers) throws Exception {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (MessageMaker mk : makers) p.send(mk.make(s));
        }
    }

    static void sendTexts(Connection c, String queue, String... texts) throws Exception {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (String t : texts) p.send(s.createTextMessage(t));
        }
    }

    /** All messages currently on {@code queue} matching {@code selector}, consumed. */
    static List<Message> take(Connection c, String queue, String selector, long firstWait) throws Exception {
        List<Message> got = new ArrayList<>();
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(queue), selector);
            Message m = mc.receive(firstWait);
            while (m != null) {
                got.add(m);
                m = mc.receive(300);
            }
        }
        return got;
    }

    static List<String> texts(List<Message> ms) throws JMSException {
        List<String> out = new ArrayList<>();
        for (Message m : ms) out.add(text(m));
        return out;
    }

    static List<String> drain(MessageConsumer c, long firstWait) throws JMSException {
        List<String> out = new ArrayList<>();
        Message m = c.receive(firstWait);
        while (m != null) {
            out.add(text(m));
            m = c.receive(300);
        }
        return out;
    }

    static String byId(String id) {
        return "p4id = '" + id + "'";
    }

    /** Receives on a transacted session and rolls back until the message stops coming; returns the deliveries. */
    static int receiveAndRollBack(Connection c, String queue) throws Exception {
        int n = 0;
        try (Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(queue));
            while (mc.receive(1500) != null) {
                n++;
                s.rollback();
                check(n < 50, "message never left the queue");
            }
        }
        return n;
    }

    static long now() {
        return System.currentTimeMillis();
    }

    // ------------------------------------------------- dead letters, redelivery

    static void dlaAfterMaxAttempts() throws Exception {
        // p4.dla.#: max-delivery-attempts 2, dead-letter-address p4.DLA
        String q = name("dla.basic");
        with(c -> {
            send(c, q, s -> tagged(s, "dead", q));
            eq(2, receiveAndRollBack(c, q), "deliveries before dead-lettering");
            List<Message> dead = take(c, "p4.DLA", byId(q), 2000);
            eq(1, dead.size(), "messages on the dead-letter address");
            Message m = dead.get(0);
            eq("dead", text(m), "body");
            eq(q, m.getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
            eq(q, m.getStringProperty("_AMQ_ORIG_QUEUE"), "_AMQ_ORIG_QUEUE");
            check(m.propertyExists("_AMQ_ORIG_MESSAGE_ID"), "_AMQ_ORIG_MESSAGE_ID is set");
            check(take(c, q, null, 300).isEmpty(), "original queue is empty");
            check(take(c, "DLQ", byId(q), 300).isEmpty(), "the catch-all DLQ is not used");
        });
    }

    static void settingsHierarchy() throws Exception {
        // p4.dla.five.# overrides max-delivery-attempts (5) and inherits p4.DLA from p4.dla.#
        String q = name("dla.five.h");
        with(c -> {
            send(c, q, s -> tagged(s, "five", q));
            eq(5, receiveAndRollBack(c, q), "deliveries before dead-lettering");
            eq(1, take(c, "p4.DLA", byId(q), 2000).size(), "dead letter on the inherited p4.DLA");
            check(take(c, "DLQ", byId(q), 300).isEmpty(), "not on the catch-all DLQ");
        });
    }

    static void dlaCatchAll() throws Exception {
        // only the "#" catch-all matches: default max-delivery-attempts 10, DLQ
        String q = name("plain.dla");
        with(c -> {
            send(c, q, s -> tagged(s, "ten", q));
            eq(10, receiveAndRollBack(c, q), "deliveries before dead-lettering");
            List<Message> dead = take(c, "DLQ", byId(q), 2000);
            eq(1, dead.size(), "messages on DLQ");
            eq(q, dead.get(0).getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
        });
    }

    static void noDlaDropped() throws Exception {
        // p4.nodla.#: empty dead-letter-address, max-delivery-attempts 2
        String q = name("nodla.x");
        with(c -> {
            send(c, q, s -> tagged(s, "gone", q));
            eq(2, receiveAndRollBack(c, q), "deliveries before dropping");
            check(take(c, q, null, 300).isEmpty(), "original queue is empty");
            check(take(c, "DLQ", byId(q), 300).isEmpty(), "not on DLQ");
            check(take(c, "p4.DLA", byId(q), 300).isEmpty(), "not on p4.DLA");
        });
    }

    static void autoCreateDeadLetterQueue() throws Exception {
        // p4.autodlq.#: dead-letter-address p4.AUTODLA, auto-create-dead-letter-resources, 1 attempt
        String a = name("autodlq.a"), b = name("autodlq.b");
        with(c -> {
            send(c, a, s -> tagged(s, "from-a", a));
            send(c, b, s -> tagged(s, "from-b", b));
            eq(1, receiveAndRollBack(c, a), "deliveries on a");
            eq(1, receiveAndRollBack(c, b), "deliveries on b");
            ClientSession.QueueQuery qa = query("DLQ." + a);
            check(qa.isExists(), "DLQ." + a + " was created");
            eq("p4.AUTODLA", String.valueOf(qa.getAddress()), "its address");
            eq("_AMQ_ORIG_ADDRESS = '" + a + "'", String.valueOf(qa.getFilterString()), "its filter");
            eq(List.of("from-a"), texts(take(c, "p4.AUTODLA::DLQ." + a, null, 2000)), "a's dead-letter queue");
            eq(List.of("from-b"), texts(take(c, "p4.AUTODLA::DLQ." + b, null, 2000)), "b's dead-letter queue");
        });
    }

    static void redeliveryDelay() throws Exception {
        // p4.redelivery.#: redelivery-delay 1000
        String q = name("redelivery.d");
        with(c -> {
            sendTexts(c, q, "r");
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            Message first = mc.receive(2000);
            eq("r", text(first), "first delivery");
            check(!first.getJMSRedelivered(), "first delivery is not redelivered");
            s.rollback();
            long t0 = now();
            check(mc.receive(600) == null, "not redelivered before the delay");
            Message again = mc.receive(4000);
            long waited = now() - t0;
            eq("r", text(again), "redelivered");
            check(waited >= 900, "redelivered after " + waited + " ms, delay is 1000");
            check(again.getJMSRedelivered(), "JMSRedelivered");
            eq(2, again.getIntProperty("JMSXDeliveryCount"), "JMSXDeliveryCount");
            s.commit();
            check(mc.receive(300) == null, "consumed");
        });
    }

    static void redeliveryDelayOthersFlow() throws Exception {
        // a message waiting out its redelivery delay does not hold back the next one
        String q = name("redelivery.others");
        with(c -> {
            sendTexts(c, q, "a");
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            eq("a", text(mc.receive(2000)), "first");
            s.rollback();
            long t0 = now();
            sendTexts(c, q, "b");
            eq("b", text(mc.receive(800)), "b flows while a waits");
            check(now() - t0 < 900, "b came before a's delay ended");
            eq("a", text(mc.receive(4000)), "a after its delay");
            s.commit();
        });
    }

    static void redeliveryBackoff() throws Exception {
        // p4.backoff.#: redelivery-delay 200, multiplier 3, max-redelivery-delay 500
        // delays: 200, 600 -> capped 500, 1800 -> capped 500
        String q = name("backoff.b");
        with(c -> {
            sendTexts(c, q, "x");
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            eq("x", text(mc.receive(2000)), "first delivery");
            long[] waited = new long[3];
            for (int i = 0; i < 3; i++) {
                s.rollback();
                long t0 = now();
                eq("x", text(mc.receive(5000)), "redelivery " + (i + 1));
                waited[i] = now() - t0;
            }
            s.commit();
            String all = Arrays.toString(waited);
            check(waited[0] >= 150 && waited[0] < 450, "first delay ~200: " + all);
            check(waited[1] >= 420 && waited[1] < 1100, "second delay ~500 (multiplied, capped): " + all);
            check(waited[2] >= 420 && waited[2] < 1300, "third delay capped at 500, not 1800: " + all);
        });
    }

    // -------------------------------------------------------------------- expiry

    static void expiryToExpiryAddress() throws Exception {
        // p4.exp.#: expiry-address p4.EXP; the broker scans every 500 ms
        String q = name("exp.a");
        with(c -> {
            long sent = now();
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setTimeToLive(200);
                p.send(tagged(s, "old", q));
            }
            Thread.sleep(1500);
            check(take(c, q, null, 500).isEmpty(), "an expired message is never delivered");
            List<Message> exp = take(c, "p4.EXP", byId(q), 2000);
            eq(1, exp.size(), "messages on the expiry address");
            Message m = exp.get(0);
            eq("old", text(m), "body");
            eq(q, m.getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
            eq(q, m.getStringProperty("_AMQ_ORIG_QUEUE"), "_AMQ_ORIG_QUEUE");
            long actual = m.getLongProperty("_AMQ_ACTUAL_EXPIRY");
            check(actual >= sent + 100 && actual <= now() + 1000, "_AMQ_ACTUAL_EXPIRY " + actual + " vs sent " + sent);
            eq(0L, m.getJMSExpiration(), "the expired copy does not expire");
        });
    }

    static void expiryCatchAll() throws Exception {
        String q = name("plain.exp");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(tagged(s, "old", q), DeliveryMode.PERSISTENT, 4, 200);
            }
            Thread.sleep(1500);
            check(take(c, q, null, 500).isEmpty(), "expired");
            eq(1, take(c, "ExpiryQueue", byId(q), 2000).size(), "on the catch-all ExpiryQueue");
        });
    }

    static void expiryInConsumerBuffer() throws Exception {
        // the message reaches the consumer's buffer and expires there: the client
        // reports it and the broker still moves it to the expiry address
        String q = name("exp.buffered");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            MessageProducer p = s.createProducer(s.createQueue(q));
            p.send(tagged(s, "soon", q), DeliveryMode.PERSISTENT, 4, 300);
            Thread.sleep(1000);
            check(mc.receive(500) == null, "expired in the buffer, not delivered");
            mc.close();
            eq(1, take(c, "p4.EXP", byId(q), 2000).size(), "moved to the expiry address");
            check(take(c, q, null, 300).isEmpty(), "gone from the queue");
        });
    }

    static void expiryDelay() throws Exception {
        // p4.expdelay.#: expiry-delay 300 applies only to messages sent without a TTL
        String q = name("expdelay.a");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.send(tagged(s, "no-ttl", q));
                p.send(tagged(s, "long-ttl", q), DeliveryMode.PERSISTENT, 4, 60_000);
            }
            Thread.sleep(1500);
            eq(List.of("long-ttl"), texts(take(c, q, null, 1000)), "remaining");
            eq(List.of("no-ttl"), texts(take(c, "p4.EXP", byId(q), 2000)), "expired by expiry-delay");
        });
    }

    static void noExpiryAddressDropped() throws Exception {
        // p4.noexp.#: empty expiry-address overrides the catch-all
        String q = name("noexp.a");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(tagged(s, "old", q), DeliveryMode.PERSISTENT, 4, 200);
            }
            Thread.sleep(1500);
            check(take(c, q, null, 500).isEmpty(), "expired");
            check(take(c, "ExpiryQueue", byId(q), 300).isEmpty(), "dropped, not on ExpiryQueue");
            eq(0L, query(q).getMessageCount(), "queue message count");
        });
    }

    static void autoCreateExpiryQueue() throws Exception {
        // p4.autoexp.#: expiry-address p4.AUTOEXP, auto-create-expiry-resources
        String q = name("autoexp.a");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(tagged(s, "old", q), DeliveryMode.PERSISTENT, 4, 200);
            }
            Thread.sleep(1500);
            ClientSession.QueueQuery xq = query("EXP." + q);
            check(xq.isExists(), "EXP." + q + " was created");
            eq("p4.AUTOEXP", String.valueOf(xq.getAddress()), "its address");
            eq("_AMQ_ORIG_ADDRESS = '" + q + "'", String.valueOf(xq.getFilterString()), "its filter");
            eq(List.of("old"), texts(take(c, "p4.AUTOEXP::EXP." + q, null, 2000)), "expired message");
        });
    }

    // --------------------------------------------------------- scheduled delivery

    static void scheduledDelivery() throws Exception {
        String q = name("sched.basic");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            MessageProducer p = s.createProducer(s.createQueue(q));
            long t0 = now();
            TextMessage later = s.createTextMessage("later");
            later.setLongProperty("_AMQ_SCHED_DELIVERY", t0 + 1200);
            p.send(later);
            p.send(s.createTextMessage("now"));
            eq("now", text(mc.receive(1000)), "the unscheduled message first");
            check(mc.receive(300) == null, "nothing before the scheduled time");
            check(now() - t0 < 1100, "checked before the scheduled time");
            eq("later", text(mc.receive(4000)), "scheduled message");
            check(now() - t0 >= 1100, "delivered at " + (now() - t0) + " ms, scheduled for 1200");
        });
    }

    static void deliveryDelayJms2() throws Exception {
        String q = name("sched.jms2");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            MessageProducer p = s.createProducer(s.createQueue(q));
            p.setDeliveryDelay(1000);
            long t0 = now();
            p.send(s.createTextMessage("delayed"));
            check(mc.receive(700) == null, "not before the delivery delay");
            Message m = mc.receive(4000);
            eq("delayed", text(m), "delayed message");
            check(now() - t0 >= 900, "delivered at " + (now() - t0) + " ms, delay 1000");
            check(m.getJMSDeliveryTime() >= t0 + 1000, "JMSDeliveryTime " + m.getJMSDeliveryTime());
        });
    }

    static void scheduledOrdering() throws Exception {
        String q = name("sched.order");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(q));
            long t0 = now();
            String[] names = {"late", "early1", "early2", "now"};
            long[] at = {t0 + 1500, t0 + 700, t0 + 700, 0};
            for (int i = 0; i < names.length; i++) {
                TextMessage m = s.createTextMessage(names[i]);
                if (at[i] > 0) m.setLongProperty("_AMQ_SCHED_DELIVERY", at[i]);
                p.send(m);
            }
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            List<String> got = new ArrayList<>();
            for (int i = 0; i < 4; i++) got.add(text(mc.receive(4000)));
            eq(List.of("now", "early1", "early2", "late"), got, "delivery order");
        });
    }

    static void scheduledTransacted() throws Exception {
        String q = name("sched.tx");
        with(c -> {
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer p = tx.createProducer(tx.createQueue(q));
            long t0 = now();
            TextMessage gone = tx.createTextMessage("rolled-back");
            gone.setLongProperty("_AMQ_SCHED_DELIVERY", t0 + 500);
            p.send(gone);
            tx.rollback();
            TextMessage kept = tx.createTextMessage("committed");
            kept.setLongProperty("_AMQ_SCHED_DELIVERY", t0 + 800);
            p.send(kept);
            tx.commit();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            check(mc.receive(400) == null, "nothing before the scheduled time");
            eq("committed", text(mc.receive(4000)), "the committed scheduled message");
            check(now() - t0 >= 700, "delivered at " + (now() - t0) + " ms, scheduled for 800");
            check(mc.receive(1000) == null, "the rolled-back one never arrives");
        });
    }

    static void scheduledSurvivesConsumerClose() throws Exception {
        String q = name("sched.close");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer first = s.createConsumer(s.createQueue(q));
            long t0 = now();
            TextMessage m = s.createTextMessage("waiting");
            m.setLongProperty("_AMQ_SCHED_DELIVERY", t0 + 1000);
            s.createProducer(s.createQueue(q)).send(m);
            check(first.receive(300) == null, "not yet");
            first.close();
            Thread.sleep(Math.max(0, t0 + 1300 - now()));
            MessageConsumer second = s.createConsumer(s.createQueue(q));
            eq("waiting", text(second.receive(3000)), "delivered to the later consumer");
        });
    }

    // ------------------------------------------------------ duplicate detection

    static MessageMaker dup(String text, String id) {
        return s -> {
            TextMessage m = s.createTextMessage(text);
            m.setStringProperty("_AMQ_DUPL_ID", id);
            return m;
        };
    }

    static void duplicateNonTx() throws Exception {
        String q = name("dup.plain");
        with(c -> {
            send(c, q, dup("m1", "a"), dup("m2", "a"), dup("m3", "b"), dup("m4", "b"), s -> s.createTextMessage("m5"));
            eq(List.of("m1", "m3", "m5"), texts(take(c, q, null, 2000)), "duplicates dropped");
        });
    }

    static void duplicateTransacted() throws Exception {
        // the duplicate is only reported at commit, which rolls the whole transaction back;
        // Artemis reports DUPLICATE_ID_REJECTED, which JMS surfaces as a plain JMSException
        String q = name("dup.tx");
        with(c -> {
            send(c, q, dup("first", "a"));
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer p = tx.createProducer(tx.createQueue(q));
            p.send(tx.createTextMessage("other"));
            p.send(dup("again", "a").make(tx));
            try {
                tx.commit();
                check(false, "commit of a transaction with a duplicate must fail");
            } catch (JMSException e) {
                check(!(e instanceof TransactionRolledBackException), "not reported as a rollback: " + e);
                check(String.valueOf(e.getMessage()).contains("Duplicate message detected"), "duplicate reported: " + e.getMessage());
            }
            eq(List.of("first"), texts(take(c, q, null, 2000)), "the whole transaction was rolled back");
            p.send(dup("third", "c").make(tx));
            tx.commit();
            eq(List.of("third"), texts(take(c, q, null, 2000)), "the session commits the next transaction");
        });
    }

    static void duplicateCacheSize() throws Exception {
        // p4.dupsmall.#: id-cache-size 3
        String q = name("dupsmall.a");
        with(c -> {
            send(c, q, dup("a1", "a"), dup("b1", "b"), dup("c1", "c"), dup("d1", "d"),
                 dup("a2", "a"), dup("d2", "d"));
            eq(List.of("a1", "b1", "c1", "d1", "a2"), texts(take(c, q, null, 2000)),
               "a fell out of the 3-entry cache, d did not");
        });
    }

    // ------------------------------------------------------------ priority, groups

    static void priorityOrdering() throws Exception {
        String q = name("prio.a");
        with(c -> {
            int[] prios = {1, 5, 9, 0, 5, 9, 4};
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < prios.length; i++) {
                    p.setPriority(prios[i]);
                    p.send(s.createTextMessage(prios[i] + "-" + i));
                }
            }
            eq(List.of("9-2", "9-5", "5-1", "5-4", "4-6", "1-0", "0-3"), texts(take(c, q, null, 2000)),
               "highest priority first, FIFO within a priority");
        });
    }

    static MessageMaker grouped(String text, String group) {
        return s -> {
            TextMessage m = s.createTextMessage(text);
            m.setStringProperty("JMSXGroupID", group);
            return m;
        };
    }

    static void groupSticky() throws Exception {
        String q = name("grp.sticky");
        with(c -> {
            Map<String, Set<Integer>> owners = new ConcurrentHashMap<>();
            CountDownLatch latch = new CountDownLatch(24);
            for (int i = 0; i < 3; i++) {
                int id = i;
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                s.createConsumer(s.createQueue(q)).setMessageListener(m -> {
                    try {
                        owners.computeIfAbsent(m.getStringProperty("JMSXGroupID"), k -> ConcurrentHashMap.newKeySet()).add(id);
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                });
            }
            MessageMaker[] ms = new MessageMaker[24];
            for (int i = 0; i < 24; i++) ms[i] = grouped("g" + i, "G" + (i % 4));
            send(c, q, ms);
            check(latch.await(10, TimeUnit.SECONDS), "all messages delivered");
            eq(4, owners.size(), "groups seen");
            for (Map.Entry<String, Set<Integer>> e : owners.entrySet())
                eq(1, e.getValue().size(), "consumers of group " + e.getKey());
        });
    }

    static void groupFirstKey() throws Exception {
        // p4.gfirst.#: default-group-first-key JMSXGroupFirstForConsumer
        String q = name("gfirst.a");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            send(c, q, grouped("a1", "A"), grouped("a2", "A"), grouped("b1", "B"), grouped("a3", "A"),
                 grouped("b2", "B"), s2 -> s2.createTextMessage("none"));
            List<String> firsts = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                Message m = mc.receive(2000);
                if (m.getBooleanProperty("JMSXGroupFirstForConsumer")) firsts.add(text(m));
            }
            eq(List.of("a1", "b1"), firsts, "messages flagged first for the consumer");
            eq("JMSXGroupFirstForConsumer", String.valueOf(query(q).getGroupFirstKey()), "queue group-first-key");
            // without group-rebalance a new consumer does not take over a group
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer other = s2.createConsumer(s2.createQueue(q));
            send(c, q, grouped("a4", "A"));
            Message m = mc.receive(2000);
            eq("a4", text(m), "group A stays with its consumer");
            check(!m.getBooleanProperty("JMSXGroupFirstForConsumer"), "not first");
            check(other.receive(300) == null, "the new consumer gets nothing");
        });
    }

    static void groupCloseBySequence() throws Exception {
        // JMSXGroupSeq = -1 closes the group; the next message starts a new one
        String q = name("gfirst.close");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            send(c, q, grouped("a1", "A"), grouped("a2", "A"), s2 -> {
                Message m = grouped("close", "A").make(s2);
                m.setIntProperty("JMSXGroupSeq", -1);
                return m;
            }, grouped("a3", "A"));
            List<String> got = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Message m = mc.receive(2000);
                got.add(text(m) + "=" + m.getBooleanProperty("JMSXGroupFirstForConsumer"));
            }
            eq(List.of("a1=true", "a2=false", "close=false", "a3=true"), got, "group-first flags");
        });
    }

    static void groupBuckets() throws Exception {
        // p4.gbucket.#: default-group-buckets 1, every group hashes to the same bucket
        String q = name("gbucket.a");
        with(c -> {
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(q));
            MessageConsumer c2 = s2.createConsumer(s2.createQueue(q));
            MessageMaker[] ms = new MessageMaker[10];
            for (int i = 0; i < 10; i++) ms[i] = grouped("m" + i, "G" + i);
            send(c, q, ms);
            int n1 = drain(c1, 1500).size(), n2 = drain(c2, 500).size();
            check((n1 == 10 && n2 == 0) || (n1 == 0 && n2 == 10), "all groups on one consumer: " + n1 + "/" + n2);
            eq(1, query(q).getGroupBuckets(), "queue group-buckets");
        });
    }

    static void groupRebalance() throws Exception {
        // p4.grebal.#: default-group-rebalance, group-first-key JMSXGroupFirstForConsumer
        String q = name("grebal.a");
        with(c -> {
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(q));
            send(c, q, grouped("a1", "A"), grouped("a2", "A"));
            Message m = c1.receive(2000);
            eq("a1", text(m), "first");
            check(m.getBooleanProperty("JMSXGroupFirstForConsumer"), "a1 first");
            m = c1.receive(2000);
            check(!m.getBooleanProperty("JMSXGroupFirstForConsumer"), "a2 not first");
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c2 = s2.createConsumer(s2.createQueue(q));
            send(c, q, grouped("a3", "A"));
            m = c1.receive(1000);
            if (m == null) m = c2.receive(2000);
            eq("a3", text(m), "after the new consumer");
            check(m.getBooleanProperty("JMSXGroupFirstForConsumer"), "the group was reassigned on consumer add");
            check(query(q).isGroupRebalance(), "queue group-rebalance");
        });
    }

    // ------------------------------------------------------------ special queues

    static void exclusiveQueue() throws Exception {
        // exclusive via the JMS destination parameter: the JMS client applies destination
        // parameters when it creates the queue for a producer (a consumer auto-creates
        // the queue on the broker with the address-settings defaults instead)
        String q = name("exq.param");
        with(c -> {
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s1.createProducer(s1.createQueue(q + "?exclusive=true")).close();
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(q + "?exclusive=true"));
            MessageConsumer c2 = s2.createConsumer(s2.createQueue(q + "?exclusive=true"));
            check(query(q).isExclusive(), "queue is exclusive");
            String[] ms = new String[10];
            for (int i = 0; i < 10; i++) ms[i] = "m" + i;
            sendTexts(c, q, ms);
            List<String> g1 = drain(c1, 1500), g2 = drain(c2, 500);
            check((g1.size() == 10 && g2.isEmpty()) || (g1.isEmpty() && g2.size() == 10),
                  "one consumer gets everything: " + g1 + " / " + g2);
            eq(Arrays.asList(ms), g1.isEmpty() ? g2 : g1, "in order");
        });
    }

    static void exclusiveFailover() throws Exception {
        // p4.excl.#: default-exclusive-queue
        String q = name("excl.fail");
        with(c -> {
            Session s1 = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(q));
            check(query(q).isExclusive(), "queue is exclusive by address settings");
            sendTexts(c, q, "m0", "m1", "m2", "m3", "m4");
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c2 = s2.createConsumer(s2.createQueue(q));
            sendTexts(c, q, "m5", "m6", "m7", "m8", "m9");
            eq("m0", text(c1.receive(2000)), "c1 first");
            Message m1 = c1.receive(2000);
            eq("m1", text(m1), "c1 second");
            m1.acknowledge();
            check(c2.receive(500) == null, "the second consumer gets nothing while the first is open");
            c1.close();
            s1.close();
            eq(List.of("m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"), drain(c2, 2000), "the rest go to the next consumer in order");
        });
    }

    static MessageMaker lv(String text, String prop, String key) {
        return s -> {
            TextMessage m = s.createTextMessage(text);
            if (key != null) m.setStringProperty(prop, key);
            return m;
        };
    }

    static void lastValueQueue() throws Exception {
        // p4.lvq.#: default-last-value-queue, keyed by _AMQ_LVQ_NAME
        String q = name("lvq.a");
        with(c -> {
            send(c, q, lv("k1-v1", "_AMQ_LVQ_NAME", "k1"), lv("k1-v2", "_AMQ_LVQ_NAME", "k1"),
                 lv("k2-v1", "_AMQ_LVQ_NAME", "k2"), lv("k1-v3", "_AMQ_LVQ_NAME", "k1"),
                 lv("plain", "_AMQ_LVQ_NAME", null));
            check(query(q).isLastValue(), "queue is last-value");
            eq(List.of("k2-v1", "k1-v3", "plain"), texts(take(c, q, null, 2000)), "only the last value per key");
        });
    }

    static void lastValueKey() throws Exception {
        // p4.lvqkey.#: default-last-value-key KEY
        String q = name("lvqkey.a");
        with(c -> {
            send(c, q, lv("x1", "KEY", "x"), lv("y1", "KEY", "y"), lv("x2", "KEY", "x"),
                 lv("lvq-name-x", "_AMQ_LVQ_NAME", "x"));
            eq("KEY", String.valueOf(query(q).getLastValueKey()), "queue last-value-key");
            // a message without KEY falls back to _AMQ_LVQ_NAME, in the same key space
            eq(List.of("y1", "lvq-name-x"), texts(take(c, q, null, 2000)), "last value per KEY");
        });
    }

    static void lastValueQueueParam() throws Exception {
        // last-value via the JMS destination parameter, the consumer arrives after the sends
        String q = name("lvparam.a");
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue dest = s.createQueue(q + "?last-value=true");
            MessageProducer p = s.createProducer(dest);
            for (int i = 0; i < 20; i++) {
                TextMessage m = s.createTextMessage("v" + i);
                m.setStringProperty("_AMQ_LVQ_NAME", "k");
                p.send(m);
            }
            MessageConsumer mc = s.createConsumer(dest);
            eq(List.of("v19"), drain(mc, 2000), "only the last value");
        });
    }

    static void nonDestructive() throws Exception {
        // p4.nd.#: default-non-destructive
        String q = name("nd.a");
        with(c -> {
            sendTexts(c, q, "m1", "m2");
            check(query(q).isNonDestructive(), "queue is non-destructive");
            eq(List.of("m1", "m2"), texts(take(c, q, null, 2000)), "first consumer");
            eq(List.of("m1", "m2"), texts(take(c, q, null, 2000)), "messages stay for the next consumer");
            eq(2L, query(q).getMessageCount(), "message count");
        });
    }

    static void nonDestructiveLastValue() throws Exception {
        // non-destructive + last-value via the Core queue configuration
        String q = name("ndlvq.a");
        createQueue(QueueConfiguration.of(q).setAddress(q).setNonDestructive(true).setLastValue(true));
        with(c -> {
            send(c, q, lv("v1", "_AMQ_LVQ_NAME", "k"), lv("v2", "_AMQ_LVQ_NAME", "k"));
            eq(List.of("v2"), texts(take(c, q, null, 2000)), "first consumer sees the last value");
            eq(List.of("v2"), texts(take(c, q, null, 2000)), "and so does the next");
            send(c, q, lv("v3", "_AMQ_LVQ_NAME", "k"));
            eq(List.of("v3"), texts(take(c, q, null, 2000)), "a newer value replaces it");
        });
    }

    static void ringQueue() throws Exception {
        // p4.ring.#: default-ring-size 3
        String q = name("ring.a");
        with(c -> {
            sendTexts(c, q, "m0", "m1", "m2", "m3", "m4");
            eq(3L, query(q).getRingSize(), "queue ring-size");
            eq(List.of("m2", "m3", "m4"), texts(take(c, q, null, 2000)), "oldest messages dropped");
        });
    }

    static void ringQueueCoreConfig() throws Exception {
        String q = name("ringcore.a");
        createQueue(QueueConfiguration.of(q).setAddress(q).setRingSize(2L));
        with(c -> {
            sendTexts(c, q, "m0", "m1", "m2", "m3");
            eq(2L, query(q).getMessageCount(), "message count held at the ring size");
            eq(List.of("m2", "m3"), texts(take(c, q, null, 2000)), "the newest two remain");
        });
    }

    static void consumersBeforeDispatch() throws Exception {
        // p4.cbd.#: default-consumers-before-dispatch 2
        String q = name("cbd.a");
        with(c -> {
            sendTexts(c, q, "m0", "m1", "m2", "m3");
            eq(2, query(q).getConsumersBeforeDispatch(), "queue consumers-before-dispatch");
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(q));
            check(c1.receive(800) == null, "one consumer is not enough");
            check(c1.receiveNoWait() == null, "not even when forced");
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c2 = s2.createConsumer(s2.createQueue(q));
            List<String> got = new ArrayList<>(drain(c1, 2000));
            got.addAll(drain(c2, 1000));
            Collections.sort(got);
            eq(List.of("m0", "m1", "m2", "m3"), got, "dispatched once the second consumer arrived");
        });
    }

    static void delayBeforeDispatch() throws Exception {
        // p4.dbd.#: consumers-before-dispatch 2, delay-before-dispatch 1500
        String q = name("dbd.a");
        with(c -> {
            sendTexts(c, q, "m0");
            eq(1500L, query(q).getDelayBeforeDispatch(), "queue delay-before-dispatch");
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            long t0 = now();
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            check(mc.receiveNoWait() == null, "held at first");
            Thread.sleep(700);
            check(mc.receiveNoWait() == null, "still held before the delay");
            Thread.sleep(Math.max(0, t0 + 1800 - now()));
            // Artemis sets no timer for the delay: the queue only re-checks it on the next
            // delivery attempt, here the forced delivery of receiveNoWait (a broker that
            // dispatches as soon as the delay ends passes too)
            Message m = mc.receiveNoWait();
            eq("m0", text(m), "dispatched to the lone consumer after the delay");
        });
    }

    // ---------------------------------------------------------------------- misc

    static void tempQueueDeletedOnClose() throws Exception {
        String[] tmp = new String[1];
        try (Connection c = cf(null).createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue t = s.createTemporaryQueue();
            tmp[0] = t.getQueueName();
            s.createProducer(t).send(s.createTextMessage("x"));
            check(query(tmp[0]).isExists(), "temporary queue exists");
            check(query(tmp[0]).isTemporary(), "and is temporary");
            // another connection can send to it
            with(o -> {
                Session os = o.createSession(false, Session.AUTO_ACKNOWLEDGE);
                os.createProducer(os.createQueue(tmp[0])).send(os.createTextMessage("y"));
            });
            eq(List.of("x", "y"), drain(s.createConsumer(t), 2000), "messages on the temporary queue");
        }
        long deadline = now() + 3000;
        while (query(tmp[0]).isExists() && now() < deadline) Thread.sleep(100);
        check(!query(tmp[0]).isExists(), "temporary queue deleted when its connection closed");
    }

    static void selectorLeavesOthers() throws Exception {
        String q = name("sel.a");
        with(c -> {
            MessageMaker[] ms = new MessageMaker[6];
            for (int i = 0; i < 6; i++) {
                int n = i;
                ms[i] = s -> {
                    TextMessage m = s.createTextMessage((n % 2 == 0 ? "red" : "blue") + n);
                    m.setStringProperty("color", n % 2 == 0 ? "red" : "blue");
                    m.setIntProperty("n", n);
                    return m;
                };
            }
            send(c, q, ms);
            eq(List.of("red0", "red2", "red4"), texts(take(c, q, "color = 'red'", 2000)), "selected");
            eq(List.of("blue3", "blue5"), texts(take(c, q, "n > 2", 2000)), "another selector");
            eq(List.of("blue1"), texts(take(c, q, null, 2000)), "the rest stayed");
        });
    }

    static void divertExclusive() throws Exception {
        String id = "excl." + RUN;
        with(c -> {
            send(c, "p4.divert.excl.src", s -> tagged(s, "d", id));
            List<Message> got = take(c, "p4.divert.excl.dst", byId(id), 2000);
            eq(1, got.size(), "diverted");
            eq("p4.divert.excl.src", got.get(0).getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
            check(take(c, "p4.divert.excl.src", byId(id), 500).isEmpty(), "an exclusive divert leaves nothing on the source");
        });
    }

    static void divertNonExclusive() throws Exception {
        String id = "copy." + RUN;
        with(c -> {
            send(c, "p4.divert.copy.src", s -> tagged(s, "d", id));
            eq(List.of("d"), texts(take(c, "p4.divert.copy.dst", byId(id), 2000)), "copied");
            eq(List.of("d"), texts(take(c, "p4.divert.copy.src", byId(id), 2000)), "original kept");
        });
    }

    static void divertFilter() throws Exception {
        String id = "filter." + RUN;
        with(c -> {
            MessageMaker[] ms = new MessageMaker[4];
            for (int i = 0; i < 4; i++) {
                String color = i % 2 == 0 ? "red" : "blue";
                String body = color + i;
                ms[i] = s -> {
                    Message m = tagged(s, body, id);
                    m.setStringProperty("color", color);
                    return m;
                };
            }
            send(c, "p4.divert.filter.src", ms);
            eq(List.of("red0", "red2"), texts(take(c, "p4.divert.filter.dst", byId(id), 2000)), "matching messages diverted");
            eq(List.of("blue1", "blue3"), texts(take(c, "p4.divert.filter.src", byId(id), 2000)), "others stay");
        });
    }

    static void divertRoutingType() throws Exception {
        // multicast source address, divert with routing-type ANYCAST into an anycast queue
        String id = "rt." + RUN;
        with(c -> {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = s.createTopic("p4.divert.rt.src");
            MessageConsumer sub = s.createConsumer(topic, byId(id));
            s.createProducer(topic).send(tagged(s, "t", id));
            eq("t", text(sub.receive(2000)), "topic subscriber still gets it");
            eq(List.of("t"), texts(take(c, "p4.divert.rt.dst", byId(id), 2000)), "anycast copy");
        });
    }

    static void consumerWindowSize() throws Exception {
        // default window: the first consumer buffers everything; window 0: nothing is buffered
        String buffered = name("window.default"), unbuffered = name("window.zero");
        with(c -> {
            sendTexts(c, buffered, "m0", "m1", "m2", "m3", "m4");
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(buffered));
            eq("m0", text(c1.receive(2000)), "first");
            Thread.sleep(300);
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            check(s2.createConsumer(s2.createQueue(buffered)).receive(500) == null, "the rest sit in the first consumer's buffer");
            eq(List.of("m1", "m2", "m3", "m4"), drain(c1, 1000), "first consumer has them");
        });
        with("consumerWindowSize=0", c -> {
            sendTexts(c, unbuffered, "m0", "m1", "m2", "m3", "m4");
            Session s1 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c1 = s1.createConsumer(s1.createQueue(unbuffered));
            eq("m0", text(c1.receive(2000)), "first");
            Session s2 = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            eq(List.of("m1", "m2", "m3", "m4"), drain(s2.createConsumer(s2.createQueue(unbuffered)), 2000),
               "a slow first consumer leaves the rest to the second");
        });
    }
}
