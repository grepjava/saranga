// Phase 7 interop suite: AMQP 1.0. Qpid JMS 1.17.0 checks behaviour, a raw
// socket checks bytes, and the Artemis Core JMS client checks what crosses
// between the protocols.
//   java -cp '<artemis>/lib/*' Amqp.java tcp://host:port [test-name...]
// The broker runs tests/interop/amqp/broker.xml:
//   CONFIG=tests/interop/amqp/broker.xml SUITE_FILE=tests/interop/amqp/Amqp.java scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.jms.Queue;
import java.util.concurrent.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

public class Amqp {
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
        for (java.lang.reflect.Method m : Amqp.class.getDeclaredMethods()) {
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

    // ------------------------------------------------------------ clients

    static String amqpUrl(String options) {
        return "amqp://" + host + ":" + port + (options.isEmpty() ? "" : "?" + options);
    }

    static Connection amqp() throws JMSException {
        return amqp("");
    }

    static Connection amqp(String options) throws JMSException {
        Connection c = new JmsConnectionFactory(amqpUrl(options)).createConnection();
        c.start();
        return c;
    }

    static Connection amqpWithClientId(String clientId) throws JMSException {
        Connection c = new JmsConnectionFactory(amqpUrl("")).createConnection();
        c.setClientID(clientId);
        c.start();
        return c;
    }

    static Connection core() throws JMSException {
        Connection c = new ActiveMQConnectionFactory(url).createConnection();
        c.start();
        return c;
    }

    static Message receive(MessageConsumer c) throws JMSException {
        Message m = c.receive(5000);
        check(m != null, "no message received");
        return m;
    }

    static void none(MessageConsumer c) throws JMSException {
        Message m = c.receive(300);
        check(m == null, "unexpected message: " + m);
    }

    // ------------------------------------------------------------ basics

    static void t_sendReceiveQueue() throws Exception {
        String q = unique("amqp.q");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            s.createProducer(queue).send(s.createTextMessage("hello"));
            Message m = receive(s.createConsumer(queue));
            eq("hello", ((TextMessage) m).getText(), "text");
            check(m.getJMSMessageID().startsWith("ID:"), "message ID " + m.getJMSMessageID());
            eq(queue, m.getJMSDestination(), "destination");
        }
    }

    static void t_messageTypes() throws Exception {
        String q = unique("amqp.types");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.send(s.createTextMessage("text"));
            BytesMessage b = s.createBytesMessage();
            b.writeBytes(new byte[] {1, 2, 3});
            p.send(b);
            MapMessage map = s.createMapMessage();
            map.setInt("i", 7);
            map.setString("s", "x");
            map.setBytes("b", new byte[] {9});
            p.send(map);
            StreamMessage st = s.createStreamMessage();
            st.writeInt(1);
            st.writeString("two");
            st.writeBoolean(true);
            p.send(st);
            p.send(s.createObjectMessage(new java.util.ArrayList<>(List.of("o"))));
            p.send(s.createMessage());
            MessageConsumer cons = s.createConsumer(queue);
            eq("text", ((TextMessage) receive(cons)).getText(), "text");
            BytesMessage rb = (BytesMessage) receive(cons);
            byte[] bytes = new byte[(int) rb.getBodyLength()];
            rb.readBytes(bytes);
            eq("[1, 2, 3]", Arrays.toString(bytes), "bytes");
            MapMessage rm = (MapMessage) receive(cons);
            eq(7, rm.getInt("i"), "map int");
            eq("x", rm.getString("s"), "map string");
            eq("[9]", Arrays.toString(rm.getBytes("b")), "map bytes");
            StreamMessage rs = (StreamMessage) receive(cons);
            eq(1, rs.readInt(), "stream int");
            eq("two", rs.readString(), "stream string");
            eq(true, rs.readBoolean(), "stream boolean");
            eq(List.of("o"), ((ObjectMessage) receive(cons)).getObject(), "object");
            Message plain = receive(cons);
            check(!(plain instanceof TextMessage) && !(plain instanceof BytesMessage), "plain message type " + plain.getClass());
        }
    }

    static void t_propertiesAndHeaders() throws Exception {
        String q = unique("amqp.props");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            Queue reply = s.createQueue(unique("amqp.reply"));
            TextMessage m = s.createTextMessage("p");
            m.setStringProperty("s", "v");
            m.setIntProperty("i", 1);
            m.setLongProperty("l", 2L);
            m.setBooleanProperty("b", true);
            m.setDoubleProperty("d", 1.5);
            m.setFloatProperty("f", 2.5f);
            m.setShortProperty("sh", (short) 3);
            m.setByteProperty("by", (byte) 4);
            m.setJMSCorrelationID("corr-1");
            m.setJMSType("my-type");
            m.setJMSReplyTo(reply);
            MessageProducer p = s.createProducer(queue);
            p.setPriority(7);
            p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            p.send(m);
            Message r = receive(s.createConsumer(queue));
            eq("v", r.getStringProperty("s"), "string");
            eq(1, r.getIntProperty("i"), "int");
            eq(2L, r.getLongProperty("l"), "long");
            eq(true, r.getBooleanProperty("b"), "boolean");
            eq(1.5, r.getDoubleProperty("d"), "double");
            eq(2.5f, r.getFloatProperty("f"), "float");
            eq((short) 3, r.getShortProperty("sh"), "short");
            eq((byte) 4, r.getByteProperty("by"), "byte");
            eq("corr-1", r.getJMSCorrelationID(), "correlation ID");
            eq("my-type", r.getJMSType(), "type");
            eq(reply, r.getJMSReplyTo(), "reply-to");
            eq(7, r.getJMSPriority(), "priority");
            eq(DeliveryMode.NON_PERSISTENT, r.getJMSDeliveryMode(), "delivery mode");
            check(r.getJMSTimestamp() > 0, "timestamp");
            eq(m.getJMSMessageID(), r.getJMSMessageID(), "message ID");
        }
    }

    static void t_topicPubSub() throws Exception {
        String t = unique("amqp.topic");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = s.createTopic(t);
            MessageConsumer a = s.createConsumer(topic);
            MessageConsumer b = s.createConsumer(topic);
            s.createProducer(topic).send(s.createTextMessage("all"));
            eq("all", ((TextMessage) receive(a)).getText(), "first subscriber");
            eq("all", ((TextMessage) receive(b)).getText(), "second subscriber");
        }
    }

    static void t_durableSubscription() throws Exception {
        String t = unique("amqp.durable");
        String clientId = unique("client");
        try (Connection c = amqpWithClientId(clientId)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createDurableSubscriber(s.createTopic(t), "sub").close();
        }
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic(t)).send(s.createTextMessage("kept"));
        }
        try (Connection c = amqpWithClientId(clientId)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber sub = s.createDurableSubscriber(s.createTopic(t), "sub");
            eq("kept", ((TextMessage) receive(sub)).getText(), "kept for the durable subscription");
            sub.close();
            s.unsubscribe("sub");
        }
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic(t)).send(s.createTextMessage("gone"));
        }
        try (Connection c = amqpWithClientId(clientId)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber sub = s.createDurableSubscriber(s.createTopic(t), "sub");
            none(sub);
            sub.close();
            s.unsubscribe("sub");
        }
    }

    static void t_durableSubscriptionQueueName() throws Exception {
        // The subscription queue is named <client ID>.<subscription name>.
        String t = unique("amqp.dname");
        String clientId = unique("client");
        try (Connection c = amqpWithClientId(clientId)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createDurableSubscriber(s.createTopic(t), "sub").close();
        }
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createTopic(t)).send(s.createTextMessage("x"));
            QueueBrowser browser = s.createBrowser(s.createQueue(t + "::" + clientId + ".sub"));
            check(browser.getEnumeration().hasMoreElements(), "subscription queue " + clientId + ".sub not found");
        }
        try (Connection c = amqpWithClientId(clientId)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.unsubscribe("sub");
        }
    }

    static void t_sharedSubscription() throws Exception {
        String t = unique("amqp.shared");
        try (Connection c1 = amqp(); Connection c2 = amqp()) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer a = s1.createSharedConsumer(s1.createTopic(t), "shared");
            MessageConsumer b = s2.createSharedConsumer(s2.createTopic(t), "shared");
            MessageProducer p = s1.createProducer(s1.createTopic(t));
            for (int i = 0; i < 10; i++) {
                p.send(s1.createTextMessage("m" + i));
            }
            int got = 0;
            for (MessageConsumer c : List.of(a, b)) {
                while (c.receive(500) != null) {
                    got++;
                }
            }
            eq(10, got, "messages shared between the consumers");
        }
    }

    static void t_sharedDurableSubscription() throws Exception {
        String t = unique("amqp.shareddurable");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createSharedDurableConsumer(s.createTopic(t), "sds").close();
            s.createProducer(s.createTopic(t)).send(s.createTextMessage("kept"));
            MessageConsumer again = s.createSharedDurableConsumer(s.createTopic(t), "sds");
            eq("kept", ((TextMessage) receive(again)).getText(), "kept for the shared durable subscription");
            again.close();
            s.unsubscribe("sds");
        }
    }

    static void t_selector() throws Exception {
        String q = unique("amqp.selector");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            for (String color : List.of("red", "blue", "red")) {
                TextMessage m = s.createTextMessage(color);
                m.setStringProperty("color", color);
                p.send(m);
            }
            MessageConsumer red = s.createConsumer(queue, "color = 'red'");
            eq("red", ((TextMessage) receive(red)).getText(), "first red");
            eq("red", ((TextMessage) receive(red)).getText(), "second red");
            none(red);
            red.close();
            eq("blue", ((TextMessage) receive(s.createConsumer(queue))).getText(), "blue left");
        }
    }

    static void t_selectorOnHeaders() throws Exception {
        String q = unique("amqp.hselector");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.send(s.createTextMessage("low"), DeliveryMode.PERSISTENT, 1, 0);
            p.send(s.createTextMessage("high"), DeliveryMode.PERSISTENT, 8, 0);
            MessageConsumer high = s.createConsumer(queue, "JMSPriority > 5");
            eq("high", ((TextMessage) receive(high)).getText(), "selected on priority");
            none(high);
        }
    }

    static void t_noLocal() throws Exception {
        String t = unique("amqp.nolocal");
        try (Connection c = amqp(); Connection other = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = s.createTopic(t);
            MessageConsumer noLocal = s.createConsumer(topic, null, true);
            s.createProducer(topic).send(s.createTextMessage("mine"));
            Session os = other.createSession(false, Session.AUTO_ACKNOWLEDGE);
            os.createProducer(os.createTopic(t)).send(os.createTextMessage("theirs"));
            eq("theirs", ((TextMessage) receive(noLocal)).getText(), "only the other connection's message");
            none(noLocal);
        }
    }

    // ------------------------------------------------------------ acks

    static void t_transactedSend() throws Exception {
        String q = unique("amqp.txsend");
        try (Connection c = amqp()) {
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = tx.createProducer(queue);
            MessageConsumer cons = s.createConsumer(queue);
            p.send(tx.createTextMessage("rolled back"));
            tx.rollback();
            p.send(tx.createTextMessage("committed"));
            none(cons);
            tx.commit();
            eq("committed", ((TextMessage) receive(cons)).getText(), "after commit");
            none(cons);
        }
    }

    static void t_transactedReceive() throws Exception {
        String q = unique("amqp.txrecv");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            s.createProducer(queue).send(s.createTextMessage("once"));
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer cons = tx.createConsumer(queue);
            Message first = receive(cons);
            check(!first.getJMSRedelivered(), "first delivery is not redelivered");
            tx.rollback();
            Message again = receive(cons);
            eq("once", ((TextMessage) again).getText(), "redelivered after rollback");
            check(again.getJMSRedelivered(), "JMSRedelivered after rollback");
            eq(2, again.getIntProperty("JMSXDeliveryCount"), "JMSXDeliveryCount");
            tx.commit();
            cons.close();
            none(s.createConsumer(queue));
        }
    }

    static void t_clientAcknowledge() throws Exception {
        String q = unique("amqp.clientack");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.send(s.createTextMessage("a"));
            p.send(s.createTextMessage("b"));
            MessageConsumer cons = s.createConsumer(queue);
            receive(cons);
            receive(cons);
            s.recover();
            Message a = receive(cons);
            eq("a", ((TextMessage) a).getText(), "redelivered after recover");
            check(a.getJMSRedelivered(), "redelivered flag");
            Message b = receive(cons);
            b.acknowledge();
        }
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            none(s.createConsumer(s.createQueue(q)));
        }
    }

    static void t_unacknowledgedReturnOnClose() throws Exception {
        String q = unique("amqp.unacked");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            s.createProducer(queue).send(s.createTextMessage("back"));
            receive(s.createConsumer(queue));
        }
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message m = receive(s.createConsumer(s.createQueue(q)));
            eq("back", ((TextMessage) m).getText(), "the unacknowledged message");
            check(m.getJMSRedelivered(), "redelivered");
        }
    }

    static void t_deadLetter() throws Exception {
        // max-delivery-attempts is 2 for amqp.dlq.# (broker.xml).
        String q = "amqp.dlq." + unique("q");
        try (Connection c = amqp()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            TextMessage m = s.createTextMessage("poison");
            m.setStringProperty("tag", q);
            p.send(m);
            s.commit();
            MessageConsumer cons = s.createConsumer(queue);
            for (int i = 0; i < 2; i++) {
                receive(cons);
                s.rollback();
            }
            none(cons);
            MessageConsumer dlq = s.createConsumer(s.createQueue("DLQ"), "tag = '" + q + "'");
            Message dead = receive(dlq);
            eq("poison", ((TextMessage) dead).getText(), "in the DLQ");
            s.commit();
        }
    }

    static void t_expiry() throws Exception {
        String q = "amqp.expiry." + unique("q");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.setTimeToLive(100);
            TextMessage m = s.createTextMessage("short-lived");
            m.setStringProperty("tag", q);
            p.send(m);
            Thread.sleep(500);
            none(s.createConsumer(queue));
            Message expired = receive(s.createConsumer(s.createQueue("ExpiryQueue"), "tag = '" + q + "'"));
            eq("short-lived", ((TextMessage) expired).getText(), "in the expiry queue");
        }
    }

    // ------------------------------------------------------------ queues

    static void t_temporaryQueue() throws Exception {
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue tq = s.createTemporaryQueue();
            s.createProducer(tq).send(s.createTextMessage("temp"));
            MessageConsumer cons = s.createConsumer(tq);
            eq("temp", ((TextMessage) receive(cons)).getText(), "temporary queue");
            cons.close();
            tq.delete();
        }
    }

    static void t_temporaryTopic() throws Exception {
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryTopic tt = s.createTemporaryTopic();
            MessageConsumer cons = s.createConsumer(tt);
            s.createProducer(tt).send(s.createTextMessage("temp topic"));
            eq("temp topic", ((TextMessage) receive(cons)).getText(), "temporary topic");
        }
    }

    static void t_requestReply() throws Exception {
        String q = unique("amqp.service");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue service = s.createQueue(q);
            MessageConsumer server = s.createConsumer(service);
            TemporaryQueue replies = s.createTemporaryQueue();
            TextMessage req = s.createTextMessage("ping");
            req.setJMSReplyTo(replies);
            s.createProducer(service).send(req);
            Message got = receive(server);
            s.createProducer(got.getJMSReplyTo()).send(s.createTextMessage("pong"));
            eq("pong", ((TextMessage) receive(s.createConsumer(replies))).getText(), "reply");
        }
    }

    static void t_browser() throws Exception {
        String q = unique("amqp.browse");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.send(s.createTextMessage("1"));
            p.send(s.createTextMessage("2"));
            List<String> seen = new ArrayList<>();
            Enumeration<?> e = s.createBrowser(queue).getEnumeration();
            while (e.hasMoreElements()) {
                seen.add(((TextMessage) e.nextElement()).getText());
            }
            eq(List.of("1", "2"), seen, "browsed");
            eq("1", ((TextMessage) receive(s.createConsumer(queue))).getText(), "still there");
        }
    }

    static void t_anonymousProducer() throws Exception {
        String q1 = unique("amqp.anon1");
        String q2 = unique("amqp.anon2");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(null);
            p.send(s.createQueue(q1), s.createTextMessage("one"));
            p.send(s.createQueue(q2), s.createTextMessage("two"));
            eq("one", ((TextMessage) receive(s.createConsumer(s.createQueue(q1)))).getText(), "first");
            eq("two", ((TextMessage) receive(s.createConsumer(s.createQueue(q2)))).getText(), "second");
        }
    }

    static void t_fullyQualifiedQueue() throws Exception {
        String a = unique("amqp.fqqn");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer sub = s.createConsumer(s.createQueue(a + "::" + a + ".q1"));
            s.createProducer(s.createQueue(a + "::" + a + ".q1")).send(s.createTextMessage("fqqn"));
            eq("fqqn", ((TextMessage) receive(sub)).getText(), "via the fully qualified name");
        }
    }

    static void t_priority() throws Exception {
        String q = unique("amqp.priority");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.send(s.createTextMessage("low"), DeliveryMode.PERSISTENT, 1, 0);
            p.send(s.createTextMessage("high"), DeliveryMode.PERSISTENT, 9, 0);
            MessageConsumer cons = s.createConsumer(queue);
            eq("high", ((TextMessage) receive(cons)).getText(), "highest first");
            eq("low", ((TextMessage) receive(cons)).getText(), "then lower");
        }
    }

    static void t_deliveryDelay() throws Exception {
        String q = unique("amqp.delay");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            // The delivery time is the client's clock; the broker's may
            // differ a little (a container's), so only the order is checked.
            p.setDeliveryDelay(1500);
            p.send(s.createTextMessage("later"));
            MessageConsumer cons = s.createConsumer(queue);
            none(cons);
            Message m = cons.receive(5000);
            check(m != null, "delivered after the delay");
        }
    }

    static void t_messageGroups() throws Exception {
        String q = unique("amqp.groups");
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageConsumer a = s.createConsumer(queue);
            MessageConsumer b = s.createConsumer(queue);
            MessageProducer p = s.createProducer(queue);
            for (int i = 0; i < 6; i++) {
                TextMessage m = s.createTextMessage("g" + i);
                m.setStringProperty("JMSXGroupID", "group-1");
                p.send(m);
            }
            int fromA = 0;
            while (a.receive(500) != null) fromA++;
            int fromB = 0;
            while (b.receive(300) != null) fromB++;
            check((fromA == 6 && fromB == 0) || (fromA == 0 && fromB == 6),
                  "a group goes to one consumer: " + fromA + " and " + fromB);
        }
    }

    static void t_receiveNoWait() throws Exception {
        // Prefetch 0: each receive asks for one message and drains.
        String q = unique("amqp.drain");
        try (Connection c = amqp("jms.prefetchPolicy.all=0")) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageConsumer cons = s.createConsumer(queue);
            check(cons.receiveNoWait() == null, "nothing yet");
            s.createProducer(queue).send(s.createTextMessage("one"));
            Message m = null;
            for (int i = 0; i < 20 && m == null; i++) {
                m = cons.receive(100);
            }
            eq("one", ((TextMessage) m).getText(), "pulled");
            check(cons.receiveNoWait() == null, "nothing more");
        }
    }

    static void t_manyMessages() throws Exception {
        String q = unique("amqp.many");
        int n = 3000;
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            MessageProducer p = s.createProducer(queue);
            p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < n; i++) {
                p.send(s.createTextMessage("m" + i));
            }
            MessageConsumer cons = s.createConsumer(queue);
            for (int i = 0; i < n; i++) {
                eq("m" + i, ((TextMessage) receive(cons)).getText(), "message " + i);
            }
        }
    }

    static void t_largeMessage() throws Exception {
        String q = unique("amqp.large");
        byte[] body = new byte[1024 * 1024];
        new Random(7).nextBytes(body);
        try (Connection c = amqp()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(q);
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(body);
            s.createProducer(queue).send(m);
            BytesMessage r = (BytesMessage) receive(s.createConsumer(queue));
            byte[] got = new byte[(int) r.getBodyLength()];
            r.readBytes(got);
            check(Arrays.equals(body, got), "large body intact");
        }
    }

    // ------------------------------------------------------------ crossing

    static void t_amqpToCore() throws Exception {
        String q = unique("amqp.tocore");
        try (Connection a = amqp(); Connection c = core()) {
            Session as = a.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = as.createProducer(as.createQueue(q));
            TextMessage t = as.createTextMessage("from amqp");
            t.setStringProperty("s", "v");
            t.setIntProperty("i", 3);
            t.setJMSCorrelationID("c1");
            t.setJMSType("t1");
            p.send(t);
            BytesMessage b = as.createBytesMessage();
            b.writeBytes(new byte[] {5, 6});
            p.send(b);
            MapMessage map = as.createMapMessage();
            map.setString("k", "v");
            p.send(map);
            Session cs = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer cons = cs.createConsumer(cs.createQueue(q));
            TextMessage rt = (TextMessage) receive(cons);
            eq("from amqp", rt.getText(), "text");
            eq("v", rt.getStringProperty("s"), "string property");
            eq(3, rt.getIntProperty("i"), "int property");
            eq("c1", rt.getJMSCorrelationID(), "correlation ID");
            eq("t1", rt.getJMSType(), "type");
            BytesMessage rb = (BytesMessage) receive(cons);
            byte[] bytes = new byte[(int) rb.getBodyLength()];
            rb.readBytes(bytes);
            eq("[5, 6]", Arrays.toString(bytes), "bytes");
            eq("v", ((MapMessage) receive(cons)).getString("k"), "map");
        }
    }

    static void t_coreToAmqp() throws Exception {
        String q = unique("amqp.fromcore");
        try (Connection a = amqp(); Connection c = core()) {
            Session cs = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = cs.createProducer(cs.createQueue(q));
            TextMessage t = cs.createTextMessage("from core");
            t.setStringProperty("s", "v");
            t.setLongProperty("l", 9L);
            t.setJMSCorrelationID("c2");
            p.send(t);
            BytesMessage b = cs.createBytesMessage();
            b.writeBytes(new byte[] {7});
            p.send(b);
            StreamMessage st = cs.createStreamMessage();
            st.writeInt(4);
            st.writeString("s");
            p.send(st);
            Session as = a.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer cons = as.createConsumer(as.createQueue(q));
            TextMessage rt = (TextMessage) receive(cons);
            eq("from core", rt.getText(), "text");
            eq("v", rt.getStringProperty("s"), "string property");
            eq(9L, rt.getLongProperty("l"), "long property");
            eq("c2", rt.getJMSCorrelationID(), "correlation ID");
            BytesMessage rb = (BytesMessage) receive(cons);
            eq(1L, rb.getBodyLength(), "bytes length");
            StreamMessage rs = (StreamMessage) receive(cons);
            eq(4, rs.readInt(), "stream int");
            eq("s", rs.readString(), "stream string");
        }
    }

    static void t_coreTopicToAmqp() throws Exception {
        String t = unique("amqp.coretopic");
        try (Connection a = amqp(); Connection c = core()) {
            Session as = a.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer sub = as.createConsumer(as.createTopic(t));
            Session cs = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            cs.createProducer(cs.createTopic(t)).send(cs.createTextMessage("to topic"));
            Message m = receive(sub);
            eq("to topic", ((TextMessage) m).getText(), "text");
            check(m.getJMSDestination() instanceof Topic, "destination is a topic: " + m.getJMSDestination());
        }
    }

    // ------------------------------------------------------------ raw

    /** Reads one frame after the 8 header bytes already read. */
    static byte[] frame(DataInputStream in) throws IOException {
        int size = in.readInt();
        byte[] rest = new byte[size - 4];
        in.readFully(rest);
        return rest;
    }

    static String text(byte[] b) {
        return new String(b, StandardCharsets.ISO_8859_1);
    }

    static void t_rawSaslAndOpen() throws Exception {
        try (Socket sock = new Socket(host, port)) {
            sock.setSoTimeout(5000);
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            DataInputStream in = new DataInputStream(sock.getInputStream());
            out.write(new byte[] {'A', 'M', 'Q', 'P', 3, 1, 0, 0});
            byte[] header = new byte[8];
            in.readFully(header);
            eq("AMQP\u0003\u0001\u0000\u0000", text(header), "SASL header");
            String mechanisms = text(frame(in));
            check(mechanisms.contains("PLAIN") && mechanisms.contains("ANONYMOUS"), "mechanisms: " + mechanisms);
            // sasl-init ANONYMOUS
            byte[] init = {0, 0, 0, 0x16, 2, 1, 0, 0, 0, 0x53, 0x41, (byte) 0xc0, 0x0c, 0x01,
                           (byte) 0xa3, 0x09, 'A', 'N', 'O', 'N', 'Y', 'M', 'O', 'U', 'S'};
            init[3] = (byte) init.length;
            out.write(init);
            byte[] outcome = frame(in);
            // sasl-outcome code ok: 00 53 44 ... 50 00
            check(text(outcome).contains("\u0000SD"), "SASL outcome");
            eq((byte) 0x50, outcome[outcome.length - 2], "outcome code type");
            eq((byte) 0, outcome[outcome.length - 1], "outcome code ok");
            out.write(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
            in.readFully(header);
            eq("AMQP\u0000\u0001\u0000\u0000", text(header), "AMQP header");
            // open with container-id "raw"
            byte[] open = {0, 0, 0, 0, 2, 0, 0, 0, 0, 0x53, 0x10, (byte) 0xc0, 0x06, 0x01, (byte) 0xa1, 0x03, 'r', 'a', 'w'};
            open[3] = (byte) open.length;
            out.write(open);
            byte[] reply = frame(in);
            String s = text(reply);
            check(s.contains("apache-activemq-artemis") || s.contains("product"), "open properties: " + s);
            check(s.contains("ANONYMOUS-RELAY"), "offers ANONYMOUS-RELAY: " + s);
        }
    }

    static void t_rawNoSaslAnonymous() throws Exception {
        // Security is disabled: a client may skip SASL.
        try (Socket sock = new Socket(host, port)) {
            sock.setSoTimeout(5000);
            OutputStream out = sock.getOutputStream();
            DataInputStream in = new DataInputStream(sock.getInputStream());
            out.write(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
            byte[] header = new byte[8];
            in.readFully(header);
            eq("AMQP\u0000\u0001\u0000\u0000", text(header), "AMQP header");
        }
    }

    // ------------------------------------------------------------ full addresses

    static final String PAD = "x".repeat(1000);

    static void takeInOrder(Connection c, String q, int n) throws JMSException {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            for (int i = 0; i < n; i++) {
                Message m = mc.receive(10000);
                check(m != null, "message " + i);
                eq(i, m.getIntProperty("i"), "in order");
            }
            none(mc);
        }
    }

    /** Sends until a send fails or 300 are sent; the count and the failure. */
    static Object[] sendUntilFailure(Connection c, String q) throws JMSException {
        int sent = 0;
        JMSException failed = null;
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageProducer p = s.createProducer(s.createQueue(q));
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
        return new Object[] {sent, failed};
    }

    // A full BLOCK address gives its producers no more link credit until it
    // has room (AMQPSessionCallback.flow): past the 1000 credits it starts
    // with, the producer waits.
    static void t_blockPolicy() throws Exception {
        String q = unique("amqp.block.q");
        try (Connection c = amqp()) {
            java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
            Thread producer = new Thread(() -> {
                try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageProducer p = s.createProducer(s.createQueue(q));
                    for (int i = 0; i < 1500; i++) {
                        TextMessage m = s.createTextMessage(i + "-" + PAD);
                        m.setIntProperty("i", i);
                        p.send(m);
                        sent.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            producer.start();
            Thread.sleep(1500);
            check(producer.isAlive(), "the producer is blocked");
            check(sent.get() < 1500, "not everything was sent: " + sent.get());
            takeInOrder(c, q, 1500);
            producer.join(10000);
            check(failure.get() == null, "producer failed: " + failure.get());
            eq(1500, sent.get(), "all sent once unblocked");
        }
    }

    // A full FAIL address rejects with resource-limit-exceeded; the link's
    // credit is drained and comes back once the address has room.
    static void t_failPolicy() throws Exception {
        String q = unique("amqp.fail.q");
        try (Connection c = amqp()) {
            Object[] r = sendUntilFailure(c, q);
            int sent = (Integer) r[0];
            JMSException failed = (JMSException) r[1];
            check(failed instanceof ResourceAllocationException, "a send failed: " + failed);
            eq("AMQ229102: Address \"" + q + "\" is full. [condition = amqp:resource-limit-exceeded]", failed.getMessage(), "the failure");
            check(sent > 10 && sent < 300, "sends before the address was full: " + sent);
            takeInOrder(c, q, sent);
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createQueue(q)).send(s.createTextMessage("again"));
            eq("again", ((TextMessage) receive(s.createConsumer(s.createQueue(q)))).getText(), "sent after draining");
        }
    }

    // Past max-size-bytes-reject-threshold a BLOCK address rejects sends
    // (isRejectingMessages).
    static void t_rejectThreshold() throws Exception {
        String q = unique("amqp.reject.q");
        try (Connection c = amqp()) {
            Object[] r = sendUntilFailure(c, q);
            int sent = (Integer) r[0];
            JMSException failed = (JMSException) r[1];
            check(failed instanceof ResourceAllocationException, "a send failed: " + failed);
            eq("Address is full: " + q + " [condition = amqp:resource-limit-exceeded]", failed.getMessage(), "the failure");
            check(sent > 10 && sent < 300, "sends before the threshold: " + sent);
            takeInOrder(c, q, sent);
        }
    }
}
