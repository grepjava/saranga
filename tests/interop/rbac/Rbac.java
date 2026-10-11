// Phase 8 interop suite: management-message-rbac. A management message
// needs manage on the management address and, with management-message-rbac,
// the view permission (a method view-permission-method-match-pattern
// matches) or the edit permission on mops.<resource>.<method>.
//   java -cp '<artemis>/lib/*' Rbac.java tcp://host:port [test-name...]
// The broker runs tests/interop/rbac/broker.xml with the users beside it:
//   CONFIG=tests/interop/rbac/broker.xml ETC=tests/interop/rbac SUITE_FILE=tests/interop/rbac/Rbac.java scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;

import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;

public class Rbac {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static ServerLocator locator;
    static ClientSessionFactory factory;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        locator = ActiveMQClient.createServerLocator(url);
        factory = locator.createSessionFactory();
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : Rbac.class.getDeclaredMethods()) {
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
        factory.close();
        locator.close();
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static void eq(Object want, Object got, String what) {
        if (!Objects.equals(want, got)) throw new AssertionError(what + ": want <" + want + "> got <" + got + ">");
    }

    /** A management client, as one user. */
    static class As implements AutoCloseable {
        final ClientSession session;
        final ClientRequestor requestor;

        As(String user) throws Exception {
            session = factory.createSession(user, user, false, true, true, false, 1);
            session.start();
            requestor = new ClientRequestor(session, "activemq.management");
        }

        ClientMessage request(ClientMessage m) throws Exception {
            ClientMessage reply = requestor.request(m, 5000);
            check(reply != null, "no reply");
            return reply;
        }

        /** The operation's result, or the error text prefixed with "!". */
        Object op(String resource, String operation, Object... params) throws Exception {
            ClientMessage m = session.createMessage(false);
            ManagementHelper.putOperationInvocation(m, resource, operation, params);
            ClientMessage reply = request(m);
            Object r = ManagementHelper.getResult(reply);
            return ManagementHelper.hasOperationSucceeded(reply) ? r : "!" + r;
        }

        /** The attribute, or the error text prefixed with "!". */
        Object attr(String resource, String attribute) throws Exception {
            ClientMessage m = session.createMessage(false);
            ManagementHelper.putAttribute(m, resource, attribute);
            ClientMessage reply = request(m);
            Object r = ManagementHelper.getResult(reply);
            return ManagementHelper.hasOperationSucceeded(reply) ? r : "!" + r;
        }

        @Override
        public void close() throws Exception {
            requestor.close();
            session.close();
        }
    }

    static void ok(Object result, String what) {
        check(!(result instanceof String s && s.startsWith("!")), what + " failed: " + result);
    }

    static void t_edit_and_view() throws Exception {
        try (As admin = new As("admin")) {
            ok(admin.attr("broker", "Version"), "Version");
            ok(admin.attr("broker", "Started"), "Started");
            ok(admin.op("broker", "listAddresses", ","), "listAddresses");
            String a = "rbac.admin." + RUN;
            ok(admin.op("broker", "createAddress", a, "ANYCAST"), "createAddress");
            ok(admin.op("broker", "deleteAddress", a), "deleteAddress");
        }
    }

    static void t_view_only() throws Exception {
        try (As viewer = new As("viewer")) {
            ok(viewer.attr("broker", "Version"), "Version");
            eq(Boolean.TRUE, viewer.attr("broker", "Started"), "Started");
            ok(viewer.op("broker", "listAddresses", ","), "listAddresses");
            ok(viewer.op("broker", "getAddressNames"), "getAddressNames");
            ok(viewer.op("queue.configured", "countMessages"), "countMessages");
            eq("!AMQ229032: User: viewer does not have permission='EDIT' on address mops.broker.createAddress",
               viewer.op("broker", "createAddress", "rbac.viewer." + RUN, "ANYCAST"), "createAddress");
            eq("!AMQ229032: User: viewer does not have permission='EDIT' on address mops.broker.resetAllMessageCounters",
               viewer.op("broker", "resetAllMessageCounters"), "resetAllMessageCounters");
        }
    }

    static void t_edit_one_queue() throws Exception {
        try (As viewer = new As("viewer")) {
            ok(viewer.op("queue.configured", "pause"), "pause");
            eq(Boolean.TRUE, viewer.attr("queue.configured", "Paused"), "Paused");
            ok(viewer.op("queue.configured", "resume"), "resume");
            eq(Boolean.FALSE, viewer.attr("queue.configured", "Paused"), "Paused");
        }
    }

    static void t_manage_only() throws Exception {
        try (As nobody = new As("nobody")) {
            eq("!Problem while retrieving attribute Version", nobody.attr("broker", "Version"), "Version");
            eq("!AMQ229032: User: nobody does not have permission='VIEW' on address mops.broker.listAddresses",
               nobody.op("broker", "listAddresses", ","), "listAddresses");
            eq("!AMQ229032: User: nobody does not have permission='VIEW' on address mops.broker.getVersion",
               nobody.op("broker", "getVersion"), "getVersion");
            eq("!AMQ229032: User: nobody does not have permission='EDIT' on address mops.queue.configured.pause",
               nobody.op("queue.configured", "pause"), "pause");
            // The resource is looked up first.
            eq("!AMQ229067: Cannot find resource with name queue.missing", nobody.op("queue.missing", "pause"), "missing");
            // A boolean attribute is read through its is method.
            eq(Boolean.TRUE, nobody.attr("broker", "Started"), "Started");
            eq(Boolean.TRUE, nobody.op("broker", "isStarted"), "isStarted");
        }
    }
}
