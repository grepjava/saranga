// Phase 6 interop suite: broker properties over broker.xml.
//   java -cp '<artemis>/lib/*' Props.java tcp://host:port [test-name...]
// The broker must run tests/interop/props/broker.properties (picked up from
// etc/ by `artemis run` and `saranga run` alike):
//   ETC=tests/interop/props SUITE=Props scripts/interop.sh [saranga|artemis]
// The properties replace the activemq login realm (users writer/wpass and
// reader/rpass, no guest), grant writers send and create and readers
// consume on props.#, pre-create the anycast queue props.a and turn off
// auto-creation under props.fixed.#.
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

public class Props {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }
    interface Action { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("realmReplaced", Props::realmReplaced);
        tests.put("propertyUsersLogIn", Props::propertyUsersLogIn);
        tests.put("rolesFromProperties", Props::rolesFromProperties);
        tests.put("precreatedQueue", Props::precreatedQueue);
        tests.put("autoCreateOff", Props::autoCreateOff);

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

    // ------------------------------------------------------------------ helpers

    static Connection connect(String user, String password) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0");
        Connection c = user == null ? cf.createConnection() : cf.createConnection(user, password);
        c.start();
        return c;
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static <T extends Throwable> T expect(Class<T> type, Action action) throws Exception {
        try {
            action.run();
        } catch (Throwable e) {
            if (type.isInstance(e)) return type.cast(e);
            throw new AssertionError("expected " + type.getSimpleName() + ", got " + e, e);
        }
        throw new AssertionError("expected " + type.getSimpleName() + ", nothing thrown");
    }

    // ------------------------------------------------------------------ tests

    // login.config's guest is gone: the realm comes from the properties.
    static void realmReplaced() throws Exception {
        expect(JMSSecurityException.class, () -> connect(null, null).close());
        expect(JMSSecurityException.class, () -> connect("writer", "wrong").close());
    }

    static void propertyUsersLogIn() throws Exception {
        connect("writer", "wpass").close();
        connect("reader", "rpass").close();
    }

    static void rolesFromProperties() throws Exception {
        String q = "props.roles." + RUN;
        try (Connection w = connect("writer", "wpass");
             Connection r = connect("reader", "rpass")) {
            Session ws = w.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session rs = r.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ws.createProducer(ws.createQueue(q)).send(ws.createTextMessage("hello"));
            expect(JMSSecurityException.class, () -> ws.createConsumer(ws.createQueue(q)));
            expect(JMSSecurityException.class,
                () -> rs.createProducer(rs.createQueue(q)).send(rs.createTextMessage("no")));
            TextMessage m = (TextMessage) rs.createConsumer(rs.createQueue(q)).receive(5000);
            check(m != null && "hello".equals(m.getText()), "reader got " + m);
        }
    }

    static void precreatedQueue() throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url + "?reconnectAttempts=0");
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession("reader", "rpass", false, true, true, false, 0)) {
            ClientSession.QueueQuery query = s.queueQuery(SimpleString.of("props.a"));
            check(query.isExists(), "props.a does not exist");
            check(!query.isAutoCreated(), "props.a was auto-created");
            check(query.getRoutingType() == org.apache.activemq.artemis.api.core.RoutingType.ANYCAST,
                "props.a is " + query.getRoutingType());
        }
    }

    // The writer may create addresses, but the address settings say no. The
    // JMS client then falls back to the 1.x name jms.queue.<name>, where the
    // writer has no rights: that is the refusal it reports.
    static void autoCreateOff() throws Exception {
        try (Connection w = connect("writer", "wpass")) {
            Session s = w.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue("props.fixed." + RUN);
            JMSSecurityException e = expect(JMSSecurityException.class,
                () -> s.createProducer(q).send(s.createTextMessage("x")));
            check(e.getMessage().contains("'CREATE_ADDRESS' on address jms.queue.props.fixed." + RUN),
                "unexpected refusal: " + e);
        }
    }
}
