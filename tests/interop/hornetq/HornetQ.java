// Phase 11 interop suite: a HornetQ 2.4.7 JMS client against a broker
// with a HORNETQ acceptor (tests/interop/hornetq/broker.xml):
//   scripts/hornetq-interop.sh [saranga|artemis]
// HornetQ clients name queues jms.queue.<name> and topics jms.topic.<name>,
// and their properties _HQ_<name> where Artemis's are _AMQ_<name>; the
// broker converts them both ways, and their HQ filter identifiers. A
// HornetQ client uses only destinations that exist, so broker.xml has them.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;

public class HornetQ {
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run(Connection c, Session s) throws Exception; }

    public static void main(String[] args) throws Exception {
        // tcp://host:port
        String[] hostPort = args[0].replaceFirst("^tcp://", "").split(":");
        Map<String, Object> params = new HashMap<>();
        params.put(TransportConstants.HOST_PROP_NAME, hostPort[0]);
        params.put(TransportConstants.PORT_PROP_NAME, Integer.parseInt(hostPort[1]));
        ConnectionFactory cf = (ConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(
            JMSFactoryType.CF, new TransportConfiguration(NettyConnectorFactory.class.getName(), params));

        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("queueRoundTrip", HornetQ::queueRoundTrip);
        tests.put("topicRoundTrip", HornetQ::topicRoundTrip);
        tests.put("durableSubscription", HornetQ::durableSubscription);
        tests.put("prioritySelector", HornetQ::prioritySelector);
        tests.put("duplicateDetection", HornetQ::duplicateDetection);
        tests.put("scheduledDelivery", HornetQ::scheduledDelivery);
        tests.put("messageGroup", HornetQ::messageGroup);
        tests.put("deadLetterOrigin", HornetQ::deadLetterOrigin);
        tests.put("largeMessage", HornetQ::largeMessage);

        int failed = 0;
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            try (Connection c = cf.createConnection()) {
                c.setClientID("hornetq-" + t.getKey() + "-" + RUN);
                c.start();
                t.getValue().run(c, c.createSession(false, Session.AUTO_ACKNOWLEDGE));
                System.out.printf("PASS %-30s%n", t.getKey());
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-30s %s%n", t.getKey(), e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void queueRoundTrip(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.q");
        MessageConsumer consumer = s.createConsumer(q);
        s.createProducer(q).send(s.createTextMessage("q"));
        TextMessage m = (TextMessage) consumer.receive(5000);
        check(m != null && m.getText().equals("q"), "received " + m);
        check(q.equals(m.getJMSDestination()), "destination " + m.getJMSDestination());
    }

    static void topicRoundTrip(Connection c, Session s) throws Exception {
        Topic t = s.createTopic("hq.t");
        MessageConsumer a = s.createConsumer(t);
        MessageConsumer b = s.createConsumer(t);
        s.createProducer(t).send(s.createTextMessage("t"));
        for (MessageConsumer mc : List.of(a, b)) {
            TextMessage m = (TextMessage) mc.receive(5000);
            check(m != null && m.getText().equals("t"), "received " + m);
            check(t.equals(m.getJMSDestination()), "destination " + m.getJMSDestination());
        }
    }

    static void durableSubscription(Connection c, Session s) throws Exception {
        Topic t = s.createTopic("hq.d");
        s.createDurableSubscriber(t, "sub").close();
        s.createProducer(t).send(s.createTextMessage("kept"));
        MessageConsumer mc = s.createDurableSubscriber(t, "sub");
        TextMessage m = (TextMessage) mc.receive(5000);
        check(m != null && m.getText().equals("kept"), "received " + m);
        mc.close();
        s.unsubscribe("sub");
    }

    // The client sends JMSPriority as HQPriority.
    static void prioritySelector(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.sel");
        MessageProducer p = s.createProducer(q);
        p.send(s.createTextMessage("low"), DeliveryMode.NON_PERSISTENT, 1, 0);
        p.send(s.createTextMessage("high"), DeliveryMode.NON_PERSISTENT, 9, 0);
        MessageConsumer mc = s.createConsumer(q, "JMSPriority > 5");
        TextMessage m = (TextMessage) mc.receive(5000);
        check(m != null && m.getText().equals("high"), "received " + m);
        check(mc.receive(500) == null, "a second message");
        mc.close();
        m = (TextMessage) s.createConsumer(q).receive(5000);
        check(m != null && m.getText().equals("low"), "left " + m);
    }

    static void duplicateDetection(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.dup");
        MessageProducer p = s.createProducer(q);
        for (int i = 0; i < 2; i++) {
            TextMessage m = s.createTextMessage("dup" + i);
            m.setStringProperty("_HQ_DUPL_ID", "id-" + RUN);
            p.send(m);
        }
        MessageConsumer mc = s.createConsumer(q);
        TextMessage m = (TextMessage) mc.receive(5000);
        check(m != null && m.getText().equals("dup0"), "received " + m);
        check(mc.receive(500) == null, "the duplicate was delivered");
        check(("id-" + RUN).equals(m.getStringProperty("_HQ_DUPL_ID")),
            "_HQ_DUPL_ID " + m.getStringProperty("_HQ_DUPL_ID"));
    }

    static void scheduledDelivery(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.sched");
        MessageConsumer mc = s.createConsumer(q);
        TextMessage m = s.createTextMessage("later");
        // The broker's clock decides; Docker's can be a second off ours.
        m.setLongProperty("_HQ_SCHED_DELIVERY", System.currentTimeMillis() + 3000);
        s.createProducer(q).send(m);
        check(mc.receive(1500) == null, "delivered before its time");
        TextMessage got = (TextMessage) mc.receive(5000);
        check(got != null && got.getText().equals("later"), "received " + got);
    }

    static void messageGroup(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.group");
        MessageConsumer mc = s.createConsumer(q);
        TextMessage m = s.createTextMessage("g");
        m.setStringProperty("JMSXGroupID", "group-" + RUN);
        s.createProducer(q).send(m);
        TextMessage got = (TextMessage) mc.receive(5000);
        check(got != null, "nothing received");
        check(("group-" + RUN).equals(got.getStringProperty("JMSXGroupID")),
            "JMSXGroupID " + got.getStringProperty("JMSXGroupID"));
        check(("group-" + RUN).equals(got.getStringProperty("_HQ_GROUP_ID")),
            "_HQ_GROUP_ID " + got.getStringProperty("_HQ_GROUP_ID"));
    }

    // Rolled back twice, the message goes to DLQ, its origin in
    // _HQ_ORIG_ADDRESS and _HQ_ORIG_QUEUE. Artemis's address has the
    // jms.queue. its delivery to this client put on the message itself
    // (applyPrefixForLegacyConsumer); Saranga's has not (COMPAT.md).
    static void deadLetterOrigin(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.dead");
        s.createProducer(q).send(s.createTextMessage("dead"));
        Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer mc = tx.createConsumer(q);
        for (int i = 0; i < 2; i++) {
            check(mc.receive(5000) != null, "attempt " + i);
            tx.rollback();
        }
        check(mc.receive(500) == null, "not dead-lettered");
        mc.close();
        Message m = s.createConsumer(s.createQueue("DLQ")).receive(5000);
        check(m != null, "nothing in DLQ");
        String address = m.getStringProperty("_HQ_ORIG_ADDRESS");
        String queue = m.getStringProperty("_HQ_ORIG_QUEUE");
        System.out.printf("     _HQ_ORIG_ADDRESS=%s _HQ_ORIG_QUEUE=%s _AMQ_ORIG_ADDRESS=%s%n",
            address, queue, m.getStringProperty("_AMQ_ORIG_ADDRESS"));
        check(address != null && address.endsWith("hq.dead"), "_HQ_ORIG_ADDRESS " + address);
        check(queue != null && queue.endsWith("hq.dead"), "_HQ_ORIG_QUEUE " + queue);
        check(m.getStringProperty("_AMQ_ORIG_ADDRESS") == null, "_AMQ_ORIG_ADDRESS is there");
    }

    static void largeMessage(Connection c, Session s) throws Exception {
        Queue q = s.createQueue("hq.large");
        byte[] body = new byte[300 * 1024];
        new Random(7).nextBytes(body);
        BytesMessage out = s.createBytesMessage();
        out.writeBytes(body);
        MessageConsumer mc = s.createConsumer(q);
        s.createProducer(q).send(out);
        BytesMessage in = (BytesMessage) mc.receive(10000);
        check(in != null, "nothing received");
        byte[] got = new byte[(int) in.getBodyLength()];
        in.readBytes(got);
        check(Arrays.equals(body, got), "body of " + got.length + " bytes differs");
    }
}
