// Phase 6 LDAP suite: logins and roles from an LDAP directory
// (tests/interop/ldap: users.ldif, login.config, broker.xml).
//   java -cp '<artemis>/lib/*' Ldap.java tcp://host:port [test-name...]
// Run it with scripts/ldap-interop.sh [saranga|artemis].

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Ldap {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }
    interface Action { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args[0];
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(1, args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("groupMember", Ldap::groupMember);
        tests.put("nestedGroups", Ldap::nestedGroups);
        tests.put("escapedUserName", Ldap::escapedUserName);
        tests.put("wrongPassword", Ldap::wrongPassword);
        tests.put("unknownUser", Ldap::unknownUser);
        tests.put("noPassword", Ldap::noPassword);
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

    static Connection open(String user, String password) throws JMSException {
        Connection c = new ActiveMQConnectionFactory(url).createConnection(user, password);
        c.start();
        return c;
    }

    static String denied(Action a) throws Exception {
        try {
            a.run();
        } catch (JMSSecurityException e) {
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

    static void send(String user, String queue) throws Exception {
        try (Connection c = open(user, user);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            s.createProducer(s.createQueue(queue)).send(s.createTextMessage(user));
        }
    }

    static String receive(String user, String password, String queue) throws Exception {
        try (Connection c = open(user, password);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Message m = s.createConsumer(s.createQueue(queue)).receive(3000);
            return m == null ? null : ((TextMessage) m).getText();
        }
    }

    static void unableToValidate(String m, String user) {
        check(m.startsWith("AMQ229031: Unable to validate user from "), m);
        check(m.endsWith(". Username: " + user + "; SSL certificate subject DN: unavailable"), m);
    }

    // ------------------------------------------------------------------ tests

    static void groupMember() throws Exception {
        // jdoe is in amq: may create the queue, send and consume.
        String q = "ldap.jdoe." + RUN;
        send("jdoe", q);
        eq("jdoe", receive("jdoe", "jdoe", q), "jdoe round trip");
    }

    static void nestedGroups() throws Exception {
        // ann is in staff, and staff is in senders: she may send and
        // consume on an existing queue, not create one.
        String q = "ldap.ann." + RUN;
        receive("jdoe", "jdoe", q);
        send("ann", q);
        eq("ann", receive("ann", "ann", q), "ann round trip");
        String m = denied(() -> receive("ann", "ann", "ldap.new." + RUN));
        eq("AMQ229213: User: ann does not have permission='CREATE_DURABLE_QUEUE' for queue ldap.new." + RUN
            + " on address ldap.new." + RUN, m, "ann creating");
    }

    static void escapedUserName() throws Exception {
        String q = "ldap.odd." + RUN;
        receive("jdoe", "jdoe", q);
        try (Connection c = open("x(y)*", "odd");
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            s.createProducer(s.createQueue(q)).send(s.createTextMessage("odd"));
        }
        eq("odd", receive("x(y)*", "odd", q), "x(y)* round trip");
    }

    static void wrongPassword() throws Exception {
        unableToValidate(denied(() -> open("jdoe", "nope").close()), "jdoe");
    }

    static void unknownUser() throws Exception {
        unableToValidate(denied(() -> open("nobody", "x").close()), "nobody");
    }

    static void noPassword() throws Exception {
        unableToValidate(denied(() -> open("jdoe", null).close()), "jdoe");
    }
}
