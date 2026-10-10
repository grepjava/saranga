// Phase 6 interop suite: configuration reload.
//   java -cp '<artemis>/lib/*' Reload.java tcp://host:port before|after
// scripts/reload-interop.sh starts the broker with
// tests/interop/reload/broker-before.xml, runs the "before" checks, puts
// broker-after.xml in its place, waits for the reload and runs the "after"
// checks.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Reload {
    static String url;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args[0];
        String phase = args[1];
        Map<String, Test> tests = new LinkedHashMap<>();
        if (phase.equals("before")) {
            tests.put("configuredQueues", Reload::configuredQueues);
            tests.put("divertBefore", () -> divertsTo("reload.kept"));
        } else {
            tests.put("queueUpdated", Reload::queueUpdated);
            tests.put("maxConsumersEnforced", Reload::maxConsumersEnforced);
            tests.put("addressesDeployedAndUndeployed", Reload::addressesDeployedAndUndeployed);
            tests.put("addressSettingsSwapped", Reload::addressSettingsSwapped);
            tests.put("divertAfter", () -> divertsTo("reload.added"));
        }
        int failed = 0;
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            try {
                t.getValue().run();
                System.out.printf("PASS %-34s%n", t.getKey());
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-34s %s%n", t.getKey(), e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    interface CoreAction { void run(ClientSession s) throws Exception; }

    static void core(CoreAction action) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url + "?reconnectAttempts=0");
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            action.run(s);
        }
    }

    static boolean exists(ClientSession s, String queue) throws Exception {
        return s.queueQuery(SimpleString.of(queue)).isExists();
    }

    static void configuredQueues() throws Exception {
        core(s -> {
            for (String q : List.of("reload.q", "reload.kept", "reload.forced.gone",
                                    "reload.forced.keep.a", "reload.forced.keep.b")) {
                check(exists(s, q), q + " missing");
            }
            ClientSession.QueueQuery q = s.queueQuery(SimpleString.of("reload.q"));
            check(q.getMaxConsumers() == -1, "max consumers " + q.getMaxConsumers());
            check(q.getFilterString() == null, "filter " + q.getFilterString());
        });
    }

    // The new filter routes, but queue queries go on reporting the old one
    // until a restart: QueueImpl.setFilter leaves the queue configuration.
    static void queueUpdated() throws Exception {
        core(s -> {
            ClientSession.QueueQuery q = s.queueQuery(SimpleString.of("reload.q"));
            check(q.getMaxConsumers() == 1, "max consumers " + q.getMaxConsumers());
            check(q.getFilterString() == null, "filter reported " + q.getFilterString());
        });
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0");
        try (Connection c = cf.createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue("reload.q");
            MessageProducer p = s.createProducer(q);
            for (String colour : List.of("blue", "red")) {
                TextMessage m = s.createTextMessage(colour);
                m.setStringProperty("colour", colour);
                p.send(m);
            }
            MessageConsumer consumer = s.createConsumer(q);
            TextMessage m = (TextMessage) consumer.receive(5000);
            check(m != null && m.getText().equals("red"), "first message " + m);
            check(consumer.receive(500) == null, "the blue one was routed");
        }
    }

    static void maxConsumersEnforced() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0");
        try (Connection c = cf.createConnection()) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue("reload.q");
            s.createConsumer(q);
            try {
                s.createConsumer(q);
                throw new AssertionError("a second consumer was allowed");
            } catch (JMSException expected) {
                check(String.valueOf(expected.getMessage()).contains("AMQ229200"),
                    "unexpected refusal: " + expected);
            }
        }
    }

    static void addressesDeployedAndUndeployed() throws Exception {
        core(s -> {
            check(exists(s, "reload.added"), "reload.added not deployed");
            check(exists(s, "reload.forced.keep.a"), "reload.forced.keep.a gone");
            check(!exists(s, "reload.forced.keep.b"), "reload.forced.keep.b not undeployed");
            check(!exists(s, "reload.forced.gone"), "reload.forced.gone not undeployed");
            check(!s.addressQuery(SimpleString.of("reload.forced.gone")).isExists(),
                "address reload.forced.gone not undeployed");
            check(exists(s, "reload.kept"), "reload.kept removed though config-delete is OFF");
        });
    }

    static void addressSettingsSwapped() throws Exception {
        core(s -> {
            ClientSession.AddressQuery a = s.addressQuery(SimpleString.of("reload.other"));
            check(!a.isAutoCreateQueues(), "auto-create-queues still on under reload.#");
        });
    }

    // A message sent to reload.src is copied to `target` by the divert.
    static void divertsTo(String target) throws Exception {
        String body = "divert " + System.nanoTime();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0");
        try (Connection c = cf.createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            s.createProducer(s.createQueue("reload.src")).send(s.createTextMessage(body));
            MessageConsumer consumer = s.createConsumer(s.createQueue(target));
            for (;;) {
                TextMessage m = (TextMessage) consumer.receive(5000);
                check(m != null, "nothing diverted to " + target);
                if (body.equals(m.getText())) break;
            }
            consumer.close();
            // Leave reload.src empty.
            MessageConsumer src = s.createConsumer(s.createQueue("reload.src"));
            while (src.receive(200) != null) { }
        }
    }
}
