// Phase 6 interop suite: a 1.x Artemis JMS client (artemis-jms-client 1.5.6)
// against a broker whose acceptor has anycastPrefix=jms.queue. and
// multicastPrefix=jms.topic. (tests/interop/prefix/broker.xml):
//   scripts/legacy-interop.sh [saranga|artemis]
// 1.x clients name queues jms.queue.<name> and topics jms.topic.<name>;
// the broker strips the prefixes and, for these clients, puts them back on
// query results and delivered messages.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Legacy {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run(Session s) throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args[0];
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("queueRoundTrip", Legacy::queueRoundTrip);
        tests.put("topicRoundTrip", Legacy::topicRoundTrip);
        tests.put("durableSubscription", Legacy::durableSubscription);
        tests.put("temporaryQueue", Legacy::temporaryQueue);

        int failed = 0;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0");
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            try (Connection c = cf.createConnection()) {
                c.setClientID("legacy-" + t.getKey() + "-" + RUN);
                c.start();
                t.getValue().run(c.createSession(false, Session.AUTO_ACKNOWLEDGE));
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

    static void queueRoundTrip(Session s) throws Exception {
        Queue q = s.createQueue("legacy.q." + RUN);
        MessageConsumer consumer = s.createConsumer(q);
        s.createProducer(q).send(s.createTextMessage("q"));
        TextMessage m = (TextMessage) consumer.receive(5000);
        check(m != null && m.getText().equals("q"), "received " + m);
        check(q.equals(m.getJMSDestination()), "destination " + m.getJMSDestination());
    }

    static void topicRoundTrip(Session s) throws Exception {
        Topic t = s.createTopic("legacy.t." + RUN);
        MessageConsumer a = s.createConsumer(t);
        MessageConsumer b = s.createConsumer(t);
        s.createProducer(t).send(s.createTextMessage("t"));
        for (MessageConsumer c : List.of(a, b)) {
            TextMessage m = (TextMessage) c.receive(5000);
            check(m != null && m.getText().equals("t"), "received " + m);
            check(t.equals(m.getJMSDestination()), "destination " + m.getJMSDestination());
        }
    }

    static void durableSubscription(Session s) throws Exception {
        Topic t = s.createTopic("legacy.d." + RUN);
        s.createDurableSubscriber(t, "sub").close();
        s.createProducer(t).send(s.createTextMessage("kept"));
        MessageConsumer c = s.createDurableSubscriber(t, "sub");
        TextMessage m = (TextMessage) c.receive(5000);
        check(m != null && m.getText().equals("kept"), "received " + m);
        c.close();
        s.unsubscribe("sub");
    }

    static void temporaryQueue(Session s) throws Exception {
        TemporaryQueue q = s.createTemporaryQueue();
        MessageConsumer c = s.createConsumer(q);
        s.createProducer(q).send(s.createTextMessage("tmp"));
        TextMessage m = (TextMessage) c.receive(5000);
        check(m != null && m.getText().equals("tmp"), "received " + m);
    }
}
