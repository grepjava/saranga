// Interop suite: AMQP broker connections reloaded.
//   java -cp '<artemis>/lib/*' BrokerConnectionReload.java <phase> <broker-port> <peer-port> <labels-file>
// scripts/broker-connection-reload-interop.sh starts a peer and a broker
// whose <broker-connections> connect to it, then reloads the broker with
// changed connections, running a phase after each step. Run it against
// the reference Java broker too: a test that fails there is a wrong test.
//
// A broker connection's receiver for queue Q is a consumer on the peer's
// queue Q; the connection it came over is named by a label kept in the
// labels file, so that a phase can tell a connection kept from one made
// again.

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonValue;
import org.apache.activemq.artemis.utils.JsonLoader;

public class BrokerConnectionReload {
    static int brokerPort, peerPort;
    static Path labelsFile;
    static Properties labels = new Properties();

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        String phase = args[0];
        brokerPort = Integer.parseInt(args[1]);
        peerPort = Integer.parseInt(args[2]);
        labelsFile = Paths.get(args[3]);
        if (Files.exists(labelsFile)) {
            try (Reader r = Files.newBufferedReader(labelsFile)) {
                labels.load(r);
            }
        }
        Map<String, Test> tests = new LinkedHashMap<>();
        switch (phase) {
            case "before" -> {
                tests.put("bothListedStarted", () -> {
                    check(started("c1") && started("c2"), "listed " + connections());
                });
                tests.put("bothConnected", () -> {
                    await(() -> connected("c1") && connected("c2"), "connected " + connections());
                });
                tests.put("receiversOnThePeer", () -> {
                    await(() -> consumers("in").size() == 1 && consumers("out").size() == 1, "consumers");
                    remember("c1", consumers("in").get(0));
                    remember("c2", consumers("out").get(0));
                });
            }
            case "changed" -> {
                tests.put("unchangedKept", () -> {
                    await(() -> consumers("in").size() == 1, "in has no consumer");
                    check(label("c1").equals(consumers("in").get(0)), "c1 made again");
                });
                tests.put("changedMadeAgain", () -> {
                    await(() -> consumers("out").size() == 1, "out has no consumer");
                    String now = consumers("out").get(0);
                    check(!label("c2").equals(now), "c2 kept");
                    remember("c2", now);
                    check(started("c2"), "c2 not started");
                });
                tests.put("addedNotAutoStarted", () -> {
                    check(listed("c3") && !started("c3"), "c3 " + connections());
                    check(consumers("a").isEmpty(), "a has a consumer");
                });
            }
            case "managed" -> {
                tests.put("startedThroughManagement", () -> {
                    op("broker", "startBrokerConnection", "c3");
                    await(() -> consumers("a").size() == 1, "a has no consumer");
                    remember("c3", consumers("a").get(0));
                });
                tests.put("stoppedThroughManagement", () -> {
                    op("broker", "stopBrokerConnection", "c2");
                    await(() -> consumers("out").isEmpty(), "out still has a consumer");
                    check(!started("c2"), "c2 started");
                });
            }
            case "restarted" -> {
                tests.put("stoppedStaysStopped", () -> {
                    check(listed("c2") && !started("c2"), "c2 " + connections());
                    Thread.sleep(1000);
                    check(consumers("out").isEmpty(), "out has a consumer");
                });
                tests.put("startedStartsAgain", () -> {
                    check(started("c3"), "c3 " + connections());
                    await(() -> consumers("a").size() == 1, "a has no consumer");
                    check(!label("c3").equals(consumers("a").get(0)), "c3 kept");
                });
            }
            case "mirrored" -> {
                tests.put("removedShutDown", () -> {
                    check(!listed("c1") && !listed("c2"), "listed " + connections());
                    await(() -> consumers("in").isEmpty(), "in still has a consumer");
                });
                tests.put("mirrorAdded", () -> {
                    await(() -> connected("c4"), "c4 " + connections());
                    await(() -> mirrorLink().size() == 1, "no mirror link");
                    remember("c4", mirrorLink().get(0));
                });
            }
            case "mirror-changed" -> {
                tests.put("mirrorNotUpdated", () -> {
                    check(connected("c4"), "c4 " + connections());
                    check(label("c4").equals(mirrorLink().get(0)), "c4 made again");
                });
            }
            case "none" -> {
                tests.put("lastRemoved", () -> {
                    check(!listed("c3"), "listed " + connections());
                    await(() -> consumers("a").isEmpty(), "a still has a consumer");
                });
                tests.put("mirrorNotRemoved", () -> {
                    check(listed("c4") && connected("c4"), "c4 " + connections());
                    check(label("c4").equals(mirrorLink().get(0)), "c4 made again");
                });
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
        try (Writer w = Files.newBufferedWriter(labelsFile)) {
            labels.store(w, null);
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    interface Condition { boolean holds() throws Exception; }

    static void await(Condition c, String message) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (c.holds()) return;
            Thread.sleep(100);
        }
        check(c.holds(), message);
    }

    static void remember(String name, String connectionId) {
        labels.setProperty(name, connectionId);
    }

    static String label(String name) {
        return labels.getProperty(name, "");
    }

    /** A management operation on the broker at `port`. */
    static Object op(int port, String resource, String operation, Object... params) throws Exception {
        try (ServerLocator l = ActiveMQClient.createServerLocator("tcp://127.0.0.1:" + port
                 + "?reconnectAttempts=0&callTimeout=5000");
             ClientSessionFactory sf = l.createSessionFactory();
             ClientSession s = sf.createSession()) {
            s.start();
            ClientRequestor requestor = new ClientRequestor(s, "activemq.management");
            ClientMessage m = s.createMessage(false);
            ManagementHelper.putOperationInvocation(m, resource, operation, params);
            ClientMessage reply = requestor.request(m);
            if (!ManagementHelper.hasOperationSucceeded(reply)) {
                throw new AssertionError(operation + " failed: " + ManagementHelper.getResult(reply));
            }
            return ManagementHelper.getResult(reply);
        }
    }

    static Object op(String resource, String operation, Object... params) throws Exception {
        return op(brokerPort, resource, operation, params);
    }

    /** listBrokerConnections, by name. */
    static Map<String, JsonObject> connections() throws Exception {
        JsonArray all = JsonLoader.readArray(new StringReader((String) op("broker", "listBrokerConnections")));
        Map<String, JsonObject> out = new TreeMap<>();
        for (JsonValue v : all) {
            JsonObject o = (JsonObject) v;
            out.put(o.getString("name"), o);
        }
        return out;
    }

    static boolean listed(String name) throws Exception {
        return connections().containsKey(name);
    }

    static boolean started(String name) throws Exception {
        JsonObject o = connections().get(name);
        return o != null && o.getBoolean("started");
    }

    static boolean connected(String name) throws Exception {
        JsonObject o = connections().get(name);
        return o != null && o.getBoolean("connected");
    }

    /** The connection IDs of the consumers on a queue at `port`. */
    static List<String> consumersAt(int port, String queue) throws Exception {
        JsonArray all = JsonLoader.readArray(new StringReader((String) op(port, "queue." + queue, "listConsumersAsJSON")));
        List<String> out = new ArrayList<>();
        for (JsonValue v : all) {
            out.add(((JsonObject) v).getString("connectionID"));
        }
        return out;
    }

    /** The peer's consumers on a queue: the broker connections' receivers. */
    static List<String> consumers(String queue) throws Exception {
        return consumersAt(peerPort, queue);
    }

    /** The mirror's link: the consumer on the broker's store-and-forward queue. */
    static List<String> mirrorLink() throws Exception {
        return consumersAt(brokerPort, "$ACTIVEMQ_ARTEMIS_MIRROR_c4");
    }
}
