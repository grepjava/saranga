// Interop suite: acceptors and jaasConfigs reloaded.
//   java -cp '<artemis>/lib/*' AcceptorReload.java <phase> <base-port> [flag-file]
// scripts/acceptor-reload-interop.sh starts the broker with
// tests/interop/reload/acceptors-before.xml and runs "before" and, in the
// background, "hold"; it then swaps in acceptors-after.xml and
// jaas.properties, writes the flag file "hold" waits for, and runs "after";
// then it empties the broker properties and runs "dropped".
// Run it against the reference Java broker too: a test that fails there is a wrong test.
//
// Ports, from the base: +0 kept, +1 changed (moved to +2 after), +3 removed,
// +4 added after, +5 autoStart=false.

import java.nio.file.*;
import java.util.*;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

public class AcceptorReload {
    static int base;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        String phase = args[0];
        base = Integer.parseInt(args[1]);
        Map<String, Test> tests = new LinkedHashMap<>();
        switch (phase) {
            case "before" -> {
                tests.put("keptListens", () -> listens(0, true));
                tests.put("changedListens", () -> listens(1, true));
                tests.put("changedNotYetMoved", () -> listens(2, false));
                tests.put("removedListens", () -> listens(3, true));
                tests.put("addedNotYet", () -> listens(4, false));
                tests.put("autoStartFalseStopped", () -> listens(5, false));
                tests.put("anonymousRefused", () -> login(0, null, false));
                tests.put("userLogsIn", () -> login(0, "a", true));
            }
            case "hold" -> hold(Paths.get(args[2]), tests);
            case "after" -> {
                tests.put("keptListens", () -> listens(0, true));
                tests.put("changedLeftOldPort", () -> listens(1, false));
                tests.put("changedMoved", () -> listens(2, true));
                tests.put("removedStopped", () -> listens(3, false));
                tests.put("addedListens", () -> listens(4, true));
                tests.put("autoStartFalseStillStopped", () -> listens(5, false));
                tests.put("guestRealmFromJaasConfigs", () -> login(0, null, true));
            }
            case "dropped" -> {
                // ConfigurationImpl installs each reload's configuration that
                // has jaasConfigs over the one before, so a realm dropped
                // later is still found there.
                tests.put("droppedRealmStillUsed", () -> login(0, null, true));
            }
            default -> throw new IllegalArgumentException(phase);
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

    static ServerLocator locator(int offset) throws Exception {
        return ActiveMQClient.createServerLocator("tcp://127.0.0.1:" + (base + offset)
            + "?reconnectAttempts=0&initialConnectAttempts=1&callTimeout=5000&connectionTTL=2000&clientFailureCheckPeriod=500");
    }

    /** A session as `user` (password the same), or without credentials. */
    static ClientSession session(ServerLocator locator, ClientSessionFactory sf, String user) throws Exception {
        return user == null ? sf.createSession() : sf.createSession(user, user, false, true, true, false, 0);
    }

    static void listens(int offset, boolean expected) throws Exception {
        boolean connected;
        try (ServerLocator l = locator(offset); ClientSessionFactory sf = l.createSessionFactory()) {
            connected = true;
        } catch (Exception e) {
            connected = false;
        }
        check(connected == expected, "port +" + offset + (expected ? " refused" : " accepted"));
    }

    static void login(int offset, String user, boolean expected) throws Exception {
        boolean in;
        try (ServerLocator l = locator(offset);
             ClientSessionFactory sf = l.createSessionFactory();
             ClientSession s = session(l, sf, user)) {
            in = true;
        } catch (Exception e) {
            in = false;
        }
        check(in == expected, (user == null ? "anonymous" : user) + (expected ? " refused" : " logged in"));
    }

    /**
     * Sessions on the kept, changed and removed acceptors, held across the
     * reload: the kept one stays, the others are closed.
     */
    static void hold(Path flag, Map<String, Test> tests) throws Exception {
        List<ServerLocator> locators = new ArrayList<>();
        Map<Integer, ClientSession> sessions = new LinkedHashMap<>();
        for (int offset : new int[] {0, 1, 3}) {
            ServerLocator l = locator(offset);
            locators.add(l);
            ClientSessionFactory sf = l.createSessionFactory();
            sessions.put(offset, session(l, sf, "a"));
        }
        System.out.println("HELD");
        System.out.flush();
        for (int i = 0; i < 600 && !Files.exists(flag); i++) {
            Thread.sleep(100);
        }
        check(Files.exists(flag), "no reload");
        Thread.sleep(3000);
        Map<Integer, Boolean> alive = new HashMap<>();
        for (Map.Entry<Integer, ClientSession> e : sessions.entrySet()) {
            boolean ok;
            try {
                e.getValue().queueQuery(SimpleString.of("acceptor.reload"));
                ok = !e.getValue().isClosed();
            } catch (Exception x) {
                ok = false;
            }
            alive.put(e.getKey(), ok);
        }
        tests.put("keptConnectionStays", () -> check(alive.get(0), "closed"));
        tests.put("changedConnectionClosed", () -> check(!alive.get(1), "still open"));
        tests.put("removedConnectionClosed", () -> check(!alive.get(3), "still open"));
        for (ServerLocator l : locators) {
            try {
                l.close();
            } catch (Exception ignored) {
            }
        }
    }
}
