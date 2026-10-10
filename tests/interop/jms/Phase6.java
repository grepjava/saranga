// Phase 6 interop suite: authentication and authorization.
//   java -cp '<artemis>/lib/*' Phase6.java tcp://host:port [test-name...]
// The broker must run the etc files of tests/interop/phase6 (broker.xml,
// login.config, users and roles):
//   ETC=tests/interop/phase6 SUITE=Phase6 scripts/interop.sh [saranga|artemis]
// Users (password = name): admin (role amq: everything), sender, receiver,
// creator, browser, fq, mapped (role external, mapped onto senders), and
// hashed (password "hashed", stored as a PBKDF2 hash). No password at all
// logs in as the guest (role guests).
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Phase6 {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }
    interface Action { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        // authentication
        tests.put("loginOk", Phase6::loginOk);
        tests.put("wrongPassword", Phase6::wrongPassword);
        tests.put("unknownUser", Phase6::unknownUser);
        tests.put("hashedPassword", Phase6::hashedPassword);
        tests.put("guest", Phase6::guest);
        tests.put("guestWithPasswordFails", Phase6::guestWithPasswordFails);
        tests.put("coreSessionLogin", Phase6::coreSessionLogin);
        tests.put("frameLimitBeforeLogin", Phase6::frameLimitBeforeLogin);
        tests.put("preAuthTopology", Phase6::preAuthTopology);
        // authorization
        tests.put("sendDenied", Phase6::sendDenied);
        tests.put("consumeDenied", Phase6::consumeDenied);
        tests.put("browse", Phase6::browse);
        tests.put("autoCreateQueueDenied", Phase6::autoCreateQueueDenied);
        tests.put("sendAutoCreateDenied", Phase6::sendAutoCreateDenied);
        tests.put("createAndDeleteQueue", Phase6::createAndDeleteQueue);
        tests.put("createAddress", Phase6::createAddress);
        tests.put("transactedSendDenied", Phase6::transactedSendDenied);
        tests.put("subscriptions", Phase6::subscriptions);
        tests.put("temporaryQueueDenied", Phase6::temporaryQueueDenied);
        tests.put("fullyQualifiedQueue", Phase6::fullyQualifiedQueue);
        tests.put("legacyExactMatch", Phase6::legacyExactMatch);
        tests.put("roleMapping", Phase6::roleMapping);
        tests.put("validatedUser", Phase6::validatedUser);

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

    static String name(String what) {
        return "p6." + what + "." + RUN;
    }

    static Connection open(String user) throws JMSException {
        return open(user, user);
    }

    static Connection open(String user, String password) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
        Connection c = user == null && password == null ? cf.createConnection() : cf.createConnection(user, password);
        c.start();
        return c;
    }

    static ClientSession core(ServerLocator locator, String user) throws Exception {
        ClientSessionFactory sf = locator.createSessionFactory();
        return sf.createSession(user, user, false, true, true, false, 0);
    }

    /** Runs the action, which must fail with a security exception; returns its message. */
    static String denied(Action action) throws Exception {
        try {
            action.run();
        } catch (JMSSecurityException e) {
            return e.getMessage();
        } catch (org.apache.activemq.artemis.api.core.ActiveMQSecurityException e) {
            return e.getMessage();
        }
        throw new AssertionError("expected a security exception");
    }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static void eq(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    static String text(Message m) throws JMSException {
        return m == null ? null : ((TextMessage) m).getText();
    }

    /** Creates an anycast queue named like its address, as admin. */
    static void adminQueue(String address, String queue) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSession s = core(locator, "admin")) {
            s.createQueue(QueueConfiguration.of(queue).setAddress(address).setRoutingType(RoutingType.ANYCAST)
                             .setDurable(true));
        } catch (org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException e) {
            // From an earlier run.
        }
    }

    static void sendAs(String user, String queue, String body) throws Exception {
        try (Connection c = open(user);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            s.createProducer(s.createQueue(queue)).send(s.createTextMessage(body));
        }
    }

    static Message receiveAs(String user, String queue) throws Exception {
        try (Connection c = open(user);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            return s.createConsumer(s.createQueue(queue)).receive(3000);
        }
    }

    static void unableToValidate(String message, String user) {
        check(message.startsWith("AMQ229031: Unable to validate user from "), message);
        check(message.endsWith(". Username: " + user + "; SSL certificate subject DN: unavailable"), message);
    }

    // ----------------------------------------------------------- authentication

    static void loginOk() throws Exception {
        String q = name("ok");
        sendAs("admin", q, "hello");
        eq("hello", text(receiveAs("admin", q)), "admin round trip");
    }

    static void wrongPassword() throws Exception {
        unableToValidate(denied(() -> open("admin", "nope").close()), "admin");
    }

    static void unknownUser() throws Exception {
        unableToValidate(denied(() -> open("nobody", "x").close()), "nobody");
    }

    static void hashedPassword() throws Exception {
        String q = name("hashed");
        sendAs("hashed", q, "h");
        eq("h", text(receiveAs("hashed", q)), "hashed user round trip");
        unableToValidate(denied(() -> open("hashed", "Hashed").close()), "hashed");
    }

    static void guest() throws Exception {
        String q = name("gd");
        sendAs(null, "p6.guest." + RUN, "g");
        eq("g", text(receiveAs(null, "p6.guest." + RUN)), "guest round trip");
        adminQueue(q, q);
        String m = denied(() -> sendAs(null, q, "x"));
        eq("AMQ229032: User: guest does not have permission='SEND' on address " + q, m, "guest send");
    }

    static void guestWithPasswordFails() throws Exception {
        unableToValidate(denied(() -> open("guest", "guest").close()), "guest");
    }

    static void coreSessionLogin() throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url)) {
            ClientSessionFactory sf = locator.createSessionFactory();
            sf.createSession("admin", "admin", false, true, true, false, 0).close();
            String m = denied(() -> sf.createSession("admin", "bad", false, true, true, false, 0));
            unableToValidate(m, "admin");
            // A failed login leaves the connection usable.
            sf.createSession("sender", "sender", false, true, true, false, 0).close();
        }
    }

    static void frameLimitBeforeLogin() throws Exception {
        java.net.URI u = new java.net.URI(url);
        // Before a login, a frame over 4 KiB closes the connection.
        try (java.net.Socket socket = new java.net.Socket(u.getHost(), u.getPort())) {
            socket.setSoTimeout(5000);
            java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream());
            out.write("ARTEMIS".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.writeInt(5000);
            out.write(new byte[5000]);
            out.flush();
            int read;
            try {
                read = socket.getInputStream().read();
            } catch (java.net.SocketException e) {
                read = -1;
            }
            eq(-1, read, "connection closed");
        }
    }

    static void preAuthTopology() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url)) {
            locator.addClusterTopologyListener(new org.apache.activemq.artemis.api.core.client.ClusterTopologyListener() {
                @Override
                public void nodeUP(org.apache.activemq.artemis.api.core.client.TopologyMember member, boolean last) {
                    events.add("up " + member.getNodeId());
                }

                @Override
                public void nodeDown(long eventUID, String nodeID) {
                    events.add("down " + nodeID);
                }
            });
            ClientSessionFactory sf = locator.createSessionFactory();
            // Before a login the broker sends a placeholder node, which the
            // client keeps to itself: nothing about the broker leaks.
            Thread.sleep(1000);
            eq(List.of(), new ArrayList<>(events), "before login");
            sf.createSession("admin", "admin", false, true, true, false, 0).close();
            for (int i = 0; i < 50 && events.isEmpty(); i++) Thread.sleep(100);
            check(!events.isEmpty(), "the real node after login");
            for (String e : events) {
                check(e.startsWith("up ") && !e.contains("00000000-0000-0000-0000-000000000000"), "real node only: " + events);
            }
            sf.close();
        }
    }

    // ------------------------------------------------------------ authorization

    static void sendDenied() throws Exception {
        String q = name("sd");
        adminQueue(q, q);
        String m = denied(() -> sendAs("receiver", q, "x"));
        eq("AMQ229032: User: receiver does not have permission='SEND' on address " + q, m, "send");
        sendAs("sender", q, "ok");
        eq("ok", text(receiveAs("receiver", q)), "sender's message");
    }

    static void consumeDenied() throws Exception {
        String q = name("cd");
        adminQueue(q, q);
        String m = denied(() -> receiveAs("sender", q));
        eq("AMQ229213: User: sender does not have permission='CONSUME' for queue " + q + " on address " + q, m, "consume");
        // browse-only users cannot consume either
        m = denied(() -> receiveAs("browser", q));
        eq("AMQ229213: User: browser does not have permission='CONSUME' for queue " + q + " on address " + q, m, "browser consume");
    }

    static void browse() throws Exception {
        String q = name("br");
        adminQueue(q, q);
        sendAs("sender", q, "b1");
        for (String user : List.of("browser", "receiver")) {
            try (Connection c = open(user);
                 Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Enumeration<?> e = s.createBrowser(s.createQueue(q)).getEnumeration();
                check(e.hasMoreElements(), user + " sees the message");
                eq("b1", text((Message) e.nextElement()), user + " browses");
            }
        }
        String m = denied(() -> {
            try (Connection c = open("sender");
                 Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createBrowser(s.createQueue(q)).getEnumeration().hasMoreElements();
            }
        });
        eq("AMQ229213: User: sender does not have permission='BROWSE' for queue " + q + " on address " + q, m, "browse");
    }

    static void autoCreateQueueDenied() throws Exception {
        String q = name("ac");
        String m = denied(() -> receiveAs("receiver", q));
        eq("AMQ229213: User: receiver does not have permission='CREATE_DURABLE_QUEUE' for queue " + q + " on address " + q, m, "auto-create");
    }

    static void sendAutoCreateDenied() throws Exception {
        String q = name("sa");
        String m = denied(() -> sendAs("sender", q, "x"));
        eq("AMQ229032: User: sender does not have permission='CREATE_ADDRESS' on address " + q, m, "send auto-create");
    }

    static void createAndDeleteQueue() throws Exception {
        String q = name("cq");
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSession creator = core(locator, "creator");
             ClientSession receiver = core(locator, "receiver")) {
            String m = denied(() -> receiver.createQueue(QueueConfiguration.of(q).setRoutingType(RoutingType.ANYCAST)));
            eq("AMQ229213: User: receiver does not have permission='CREATE_DURABLE_QUEUE' for queue " + q + " on address " + q, m, "create");
            creator.createQueue(QueueConfiguration.of(q).setRoutingType(RoutingType.ANYCAST));
            m = denied(() -> receiver.deleteQueue(q));
            eq("AMQ229213: User: receiver does not have permission='DELETE_DURABLE_QUEUE' for queue " + q + " on address " + q, m, "delete");
            creator.deleteQueue(q);
            check(!creator.queueQuery(SimpleString.of(q)).isExists(), "deleted");
            // Non-durable queues need the non-durable permissions, and the
            // address (auto-deleted with its queue) needs creating again.
            String nd = name("cqnd");
            QueueConfiguration ndq = QueueConfiguration.of(nd).setRoutingType(RoutingType.ANYCAST).setDurable(false)
                                                       .setAddress(q);
            m = denied(() -> receiver.createQueue(ndq));
            eq("AMQ229213: User: receiver does not have permission='CREATE_ADDRESS' for queue " + nd + " on address " + q, m, "create address for the queue");
            creator.createAddress(SimpleString.of(q), RoutingType.ANYCAST, false);
            receiver.createQueue(ndq);
            receiver.deleteQueue(nd);
        }
    }

    static void createAddress() throws Exception {
        String a = name("ca");
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSession creator = core(locator, "creator");
             ClientSession receiver = core(locator, "receiver")) {
            String m = denied(() -> receiver.createAddress(SimpleString.of(a), RoutingType.MULTICAST, false));
            eq("AMQ229032: User: receiver does not have permission='CREATE_ADDRESS' on address " + a, m, "create address");
            creator.createAddress(SimpleString.of(a), RoutingType.MULTICAST, false);
            check(creator.addressQuery(SimpleString.of(a)).isExists(), "address created");
        }
    }

    static void transactedSendDenied() throws Exception {
        String q = name("tx");
        adminQueue(q, q);
        try (Connection c = open("receiver");
             Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
            // Transacted sends do not wait for the broker: the refusal marks
            // the transaction rollback-only, and the commit fails with it.
            s.createProducer(s.createQueue(q)).send(s.createTextMessage("x"));
            String m = denied(s::commit);
            eq("AMQ229032: User: receiver does not have permission='SEND' on address " + q, m, "commit");
        }
        check(receiveAs("admin", q) == null, "nothing sent");
    }

    static void subscriptions() throws Exception {
        String t = name("sub");
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSession admin = core(locator, "admin")) {
            admin.createAddress(SimpleString.of(t), RoutingType.MULTICAST, false);
        }
        try (Connection c = open("receiver");
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            // A plain subscription is a non-durable queue: allowed.
            MessageConsumer sub = s.createConsumer(s.createTopic(t));
            sendTopic("sender", t, "news");
            eq("news", text(sub.receive(3000)), "subscriber");
            String m = denied(() -> s.createSharedDurableConsumer(s.createTopic(t), "d" + RUN));
            eq("AMQ229213: User: receiver does not have permission='CREATE_DURABLE_QUEUE' for queue d" + RUN + " on address " + t, m, "durable");
        }
    }

    static void sendTopic(String user, String topic, String body) throws Exception {
        try (Connection c = open(user);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            s.createProducer(s.createTopic(topic)).send(s.createTextMessage(body));
        }
    }

    static void temporaryQueueDenied() throws Exception {
        try (Connection c = open("sender");
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            String m = denied(() -> s.createTemporaryQueue());
            check(m.startsWith("AMQ229213: User: sender does not have permission='CREATE_NON_DURABLE_QUEUE' for queue "), m);
        }
        try (Connection c = open("admin");
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            TemporaryQueue tq = s.createTemporaryQueue();
            s.createProducer(tq).send(s.createTextMessage("t"));
            eq("t", text(s.createConsumer(tq).receive(3000)), "admin temporary queue");
        }
    }

    static void fullyQualifiedQueue() throws Exception {
        String address = "p6.fq", queue = "p6.fq.special";
        adminQueue(address, queue);
        String fqqn = address + "::" + queue;
        sendAs("sender", fqqn, "fq");
        String m = denied(() -> receiveAs("receiver", fqqn));
        eq("AMQ229213: User: receiver does not have permission='CONSUME' for queue " + queue + " on address " + address, m, "fqqn");
        eq("fq", text(receiveAs("fq", fqqn)), "fq user consumes");
    }

    static void legacyExactMatch() throws Exception {
        String address = "p6.legacy", queue = "p6.legacy.q";
        adminQueue(address, queue);
        String fqqn = address + "::" + queue;
        sendAs("sender", fqqn, "legacy");
        // Only a bare queue name gets the second check against
        // `address.queue`; with an FQQN it would be `address.address::queue`.
        // (Core consumers: the JMS client would look for an address named
        // like the queue.)
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSession sender = core(locator, "sender");
             ClientSession fq = core(locator, "fq")) {
            String m = denied(() -> sender.createConsumer(queue));
            eq("AMQ229213: User: sender does not have permission='CONSUME' for queue " + queue + " on address " + address + "." + queue, m, "legacy");
            m = denied(() -> fq.createConsumer(fqqn));
            eq("AMQ229213: User: fq does not have permission='CONSUME' for queue " + queue + " on address " + address, m, "fqqn skips the legacy match");
            fq.start();
            org.apache.activemq.artemis.api.core.client.ClientMessage got = fq.createConsumer(queue).receive(3000);
            check(got != null, "address.queue match grants fq");
            got.acknowledge();
        }
    }

    static void roleMapping() throws Exception {
        String q = name("rm");
        adminQueue(q, q);
        sendAs("mapped", q, "mapped");
        eq("mapped", text(receiveAs("receiver", q)), "external role mapped onto senders");
    }

    static void validatedUser() throws Exception {
        String q = name("vu");
        adminQueue(q, q);
        sendAs("sender", q, "v");
        Message m = receiveAs("receiver", q);
        eq("sender", m.getStringProperty("JMSXUserID"), "JMSXUserID");
        eq("sender", m.getStringProperty("_AMQ_VALIDATED_USER"), "_AMQ_VALIDATED_USER");
        sendAs(null, "p6.guest.vu" + RUN, "g");
        eq("guest", receiveAs(null, "p6.guest.vu" + RUN).getStringProperty("_AMQ_VALIDATED_USER"), "guest's messages");
    }
}
