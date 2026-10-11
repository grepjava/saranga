// Phase 8 interop suite: management through messages sent to
// activemq.management, as the Artemis CLI, ClientRequestor and
// JMSManagementHelper send them. Ported in part from Artemis's
// QueueControlTest, AddressControlTest, ActiveMQServerControlTest and
// their *UsingCoreTest variants.
//   java -cp '<artemis>/lib/*' Management.java tcp://host:port [test-name...]
// The broker runs tests/interop/management/broker.xml:
//   CONFIG=tests/interop/management/broker.xml SUITE_FILE=tests/interop/management/Management.java scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.net.URI;
import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;

public class Management {
    static String url;
    static String host;
    static int port;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int counter;
    static ServerLocator locator;
    static ClientSessionFactory factory;
    static ClientSession session;
    static ClientRequestor requestor;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        URI u = new URI(url);
        host = u.getHost();
        port = u.getPort();
        locator = ActiveMQClient.createServerLocator(url);
        factory = locator.createSessionFactory();
        session = factory.createSession();
        session.start();
        requestor = new ClientRequestor(session, "activemq.management");
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : Management.class.getDeclaredMethods()) {
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
        session.close();
        factory.close();
        locator.close();
        System.exit(failed == 0 ? 0 : 1);
    }

    // ------------------------------------------------------------ helpers

    static void check(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static void eq(Object want, Object got, String what) {
        if (!Objects.equals(want, got)) throw new AssertionError(what + ": want <" + want + "> got <" + got + ">");
    }

    static String name(String what) {
        return "mgmt." + what + "." + RUN + "." + (++counter);
    }

    static ClientMessage request(ClientMessage m) throws Exception {
        ClientMessage reply = requestor.request(m, 5000);
        check(reply != null, "no reply");
        return reply;
    }

    /** Invokes an operation; fails the test if it fails. */
    static Object op(String resource, String operation, Object... params) throws Exception {
        ClientMessage m = session.createMessage(false);
        ManagementHelper.putOperationInvocation(m, resource, operation, params);
        ClientMessage reply = request(m);
        if (!ManagementHelper.hasOperationSucceeded(reply)) {
            throw new AssertionError(operation + " failed: " + ManagementHelper.getResult(reply));
        }
        return ManagementHelper.getResult(reply);
    }

    /** Invokes an operation that must fail; returns the error text. */
    static String opFails(String resource, String operation, Object... params) throws Exception {
        ClientMessage m = session.createMessage(false);
        ManagementHelper.putOperationInvocation(m, resource, operation, params);
        ClientMessage reply = request(m);
        check(!ManagementHelper.hasOperationSucceeded(reply), operation + " succeeded");
        return String.valueOf(ManagementHelper.getResult(reply));
    }

    static Object attr(String resource, String attribute) throws Exception {
        ClientMessage m = session.createMessage(false);
        ManagementHelper.putAttribute(m, resource, attribute);
        ClientMessage reply = request(m);
        if (!ManagementHelper.hasOperationSucceeded(reply)) {
            throw new AssertionError(attribute + " failed: " + ManagementHelper.getResult(reply));
        }
        return ManagementHelper.getResult(reply);
    }

    static long longAttr(String resource, String attribute) throws Exception {
        return ((Number) attr(resource, attribute)).longValue();
    }

    static String queue(String name) {
        return "queue." + name;
    }

    /** An anycast queue on its own address. */
    static String newQueue(String what) throws Exception {
        String q = name(what);
        session.createQueue(QueueConfiguration.of(q).setRoutingType(RoutingType.ANYCAST));
        return q;
    }

    static void send(String address, int n, Map<String, Object> props) throws Exception {
        try (ClientProducer p = session.createProducer(address)) {
            for (int i = 0; i < n; i++) {
                ClientMessage m = session.createMessage(true);
                m.getBodyBuffer().writeNullableSimpleString(SimpleString.of("body " + i));
                m.putIntProperty("i", i);
                if (props != null) {
                    for (Map.Entry<String, Object> e : props.entrySet()) m.putObjectProperty(e.getKey(), e.getValue());
                }
                p.send(m);
            }
        }
    }

    static void send(String address, int n) throws Exception {
        send(address, n, null);
    }

    static long count(String q) throws Exception {
        return longAttr(queue(q), "MessageCount");
    }

    /** Waits for a queue's message count (moves and copies land in the background on some brokers). */
    static void waitCount(String q, long want) throws Exception {
        long got = -1;
        for (int i = 0; i < 50; i++) {
            got = count(q);
            if (got == want) return;
            Thread.sleep(50);
        }
        eq(want, got, "message count of " + q);
    }

    static List<ClientMessage> receive(String q, int n) throws Exception {
        List<ClientMessage> out = new ArrayList<>();
        try (ClientConsumer c = session.createConsumer(q)) {
            for (int i = 0; i < n; i++) {
                ClientMessage m = c.receive(2000);
                if (m == null) break;
                m.acknowledge();
                out.add(m);
            }
            session.commit();
        }
        return out;
    }

    // ------------------------------------------------------------ errors and the broker

    static void t_unknown_resource() throws Exception {
        String e = opFails("queue.nope." + RUN, "pause");
        eq("AMQ229067: Cannot find resource with name queue.nope." + RUN, e, "error");
    }

    static void t_unknown_operation() throws Exception {
        String e = opFails("broker", "noSuchOperation", 1, "x");
        eq("AMQ229069: no operation noSuchOperation/2", e, "error");
    }

    static void t_unknown_attribute() throws Exception {
        ClientMessage m = session.createMessage(false);
        ManagementHelper.putAttribute(m, "broker", "NoSuchAttribute");
        ClientMessage reply = request(m);
        check(!ManagementHelper.hasOperationSucceeded(reply), "succeeded");
        eq("Problem while retrieving attribute NoSuchAttribute", ManagementHelper.getResult(reply), "error");
    }

    static void t_wrong_parameter_types() throws Exception {
        String e = opFails("broker", "createAddress", 1, 2);
        eq("AMQ229069: no operation createAddress/2", e, "error");
    }

    static void t_broker_attributes() throws Exception {
        eq(true, attr("broker", "Started"), "Started");
        eq(true, attr("broker", "Active"), "Active");
        eq("management", attr("broker", "Name"), "Name");
        eq("activemq.management", attr("broker", "ManagementAddress"), "ManagementAddress");
        eq("activemq.notifications", attr("broker", "ManagementNotificationAddress"), "ManagementNotificationAddress");
        eq(true, attr("broker", "PersistenceEnabled"), "PersistenceEnabled");
        eq(false, attr("broker", "SecurityEnabled"), "SecurityEnabled");
        check(!((String) attr("broker", "NodeID")).isEmpty(), "NodeID");
        check(((Number) attr("broker", "UptimeMillis")).longValue() > 0, "UptimeMillis");
        eq(90L, ((Number) attr("broker", "MaxDiskUsage")).longValue(), "MaxDiskUsage");
        double disk = ((Number) attr("broker", "DiskStoreUsage")).doubleValue();
        check(disk >= 0 && disk < 1, "DiskStoreUsage " + disk);
        List<Object> queues = Arrays.asList((Object[]) attr("broker", "QueueNames"));
        check(queues.contains("configured") && queues.contains("DLQ"), "QueueNames " + queues);
        List<Object> addresses = Arrays.asList((Object[]) attr("broker", "AddressNames"));
        check(addresses.contains("configured"), "AddressNames " + addresses);
    }

    static void t_getter_as_operation() throws Exception {
        eq("management", op("broker", "getName"), "getName");
        eq(true, op("broker", "isStarted"), "isStarted");
    }

    static void t_correlation_id() throws Exception {
        ClientMessage m = session.createMessage(false);
        ManagementHelper.putAttribute(m, "broker", "Name");
        m.setCorrelationID("corr-" + RUN);
        ClientMessage reply = request(m);
        eq("corr-" + RUN, String.valueOf(reply.getCorrelationID()), "correlation id");
    }

    // ------------------------------------------------------------ addresses

    static void t_create_update_delete_address() throws Exception {
        String a = name("address");
        eq("Address [name=" + a + ", routingTypes={ANYCAST}, autoCreated=false]",
           op("broker", "createAddress", a, "ANYCAST"), "createAddress");
        eq("Address [name=" + a + ", routingTypes={ANYCAST}, autoCreated=false]",
           op("broker", "getAddressInfo", a), "getAddressInfo");
        eq("Address [name=" + a + ", routingTypes={MULTICAST,ANYCAST}, autoCreated=false]",
           op("broker", "updateAddress", a, "ANYCAST,MULTICAST"), "updateAddress");
        check(opFails("broker", "createAddress", a, "ANYCAST").startsWith("AMQ229204"), "exists");
        op("broker", "deleteAddress", a);
        check(opFails("broker", "getAddressInfo", a).startsWith("AMQ229203"), "gone");
    }

    static void t_delete_address_with_bindings() throws Exception {
        String a = name("bound");
        op("broker", "createAddress", a, "ANYCAST");
        op("broker", "createQueue", a, "ANYCAST", a, null, true, -1, false, false);
        eq("AMQ229205: Address " + a + " has bindings", opFails("broker", "deleteAddress", a), "error");
        op("broker", "deleteAddress", a, true);
        check(!Arrays.asList((Object[]) attr("broker", "QueueNames")).contains(a), "queue removed");
    }

    static void t_address_attributes() throws Exception {
        String a = name("attrs");
        op("broker", "createAddress", a, "MULTICAST");
        String r = "address." + a;
        eq(0L, longAttr(r, "QueueCount"), "QueueCount");
        send(a, 2);
        eq(2L, longAttr(r, "UnRoutedMessageCount"), "UnRoutedMessageCount");
        session.createQueue(QueueConfiguration.of(a + ".q1").setAddress(a).setRoutingType(RoutingType.MULTICAST));
        session.createQueue(QueueConfiguration.of(a + ".q2").setAddress(a).setRoutingType(RoutingType.MULTICAST));
        send(a, 3);
        eq(3L, longAttr(r, "RoutedMessageCount"), "RoutedMessageCount");
        eq(6L, longAttr(r, "MessageCount"), "MessageCount");
        eq(2L, longAttr(r, "QueueCount"), "QueueCount");
        List<Object> names = Arrays.asList((Object[]) attr(r, "QueueNames"));
        check(names.contains(a + ".q1") && names.contains(a + ".q2"), "QueueNames " + names);
        eq("[\"MULTICAST\"]", attr(r, "RoutingTypesAsJSON"), "RoutingTypesAsJSON");
        eq(false, attr(r, "Paging"), "Paging");
        eq(false, attr(r, "AutoCreated"), "AutoCreated");
        eq(6L, ((Number) op(r, "purge")).longValue(), "purge");
        eq(0L, longAttr(r, "MessageCount"), "MessageCount after purge");
    }

    static void t_address_pause() throws Exception {
        String q = newQueue("addresspause");
        op("address." + q, "pause");
        eq(true, attr("address." + q, "Paused"), "address Paused");
        eq(true, attr(queue(q), "Paused"), "queue Paused");
        send(q, 1);
        try (ClientConsumer c = session.createConsumer(q)) {
            check(c.receive(500) == null, "delivered while paused");
            op("address." + q, "resume");
            ClientMessage m = c.receive(2000);
            check(m != null, "not delivered after resume");
            m.acknowledge();
        }
    }

    static void t_address_send_message() throws Exception {
        String q = newQueue("addresssend");
        String id = (String) op("address." + q, "sendMessage", Map.of("color", "red"), 3, "hello", true, null, null);
        check(Long.parseLong(id) > 0, "id " + id);
        ClientMessage m = receive(q, 1).get(0);
        eq("red", m.getStringProperty("color"), "header");
        eq("hello", m.getBodyBuffer().readNullableSimpleString().toString(), "body");
        eq(true, m.isDurable(), "durable");
    }

    static void t_list_addresses() throws Exception {
        String a = name("listed");
        op("broker", "createAddress", a, "ANYCAST");
        String sep = (String) op("broker", "listAddresses", "|");
        check(Arrays.asList(sep.split("\\|")).contains(a), "listAddresses(separator)");
        String json = (String) op("broker", "listAddresses",
            "{\"field\":\"name\",\"operation\":\"EQUALS\",\"value\":\"" + a + "\",\"sortField\":\"name\",\"sortOrder\":\"asc\"}", 1, 10);
        JsonObject o = JsonUtil.readJsonObject(json);
        eq(1, o.getInt("count"), "count");
        JsonObject first = o.getJsonArray("data").getJsonObject(0);
        eq(a, first.getString("name"), "name");
        eq("[\"ANYCAST\"]", first.getString("routingTypes"), "routingTypes");
        eq("0", first.getString("queueCount"), "queueCount");
    }

    // ------------------------------------------------------------ queues

    static void t_create_queue_text() throws Exception {
        String q = name("created");
        String text = (String) op("broker", "createQueue", q, "ANYCAST", q, "color = 'red'", true, 5, false, true);
        eq("Queue [name=" + q + ", address=" + q + ", routingType=ANYCAST, filter=color = 'red', durable=true, maxConsumers=5, purgeOnNoConsumers=false, autoCreateAddress=false, exclusive=false, lastValue=false, lastValueKey=null, nonDestructive=false, consumersBeforeDispatch=0, delayBeforeDispatch=-1, autoCreateAddress=false]",
           text, "createQueue");
        eq("color = 'red'", attr(queue(q), "Filter"), "Filter");
        eq(5L, longAttr(queue(q), "MaxConsumers"), "MaxConsumers");
        eq("ANYCAST", attr(queue(q), "RoutingType"), "RoutingType");
        eq(true, attr(queue(q), "Durable"), "Durable");
        String e = opFails("broker", "createQueue", q, "ANYCAST", q, null, true, -1, false, true);
        check(e.startsWith("AMQ229019"), "exists: " + e);
        op("broker", "destroyQueue", q);
        eq("AMQ229017: Queue " + q + " does not exist", opFails("broker", "destroyQueue", q), "gone");
    }

    static void t_create_queue_json() throws Exception {
        String q = name("json");
        String json = (String) op("broker", "createQueue",
            "{\"name\":\"" + q + "\",\"address\":\"" + q + "\",\"routing-type\":\"ANYCAST\",\"max-consumers\":3,\"exclusive\":true}");
        JsonObject o = JsonUtil.readJsonObject(json);
        eq(q, o.getString("name"), "name");
        eq(true, attr(queue(q), "Exclusive"), "Exclusive");
        eq(3L, longAttr(queue(q), "MaxConsumers"), "MaxConsumers");
        op("broker", "createQueue", "{\"name\":\"" + q + "\",\"address\":\"" + q + "\",\"routing-type\":\"ANYCAST\"}", true);
    }

    static void t_update_queue() throws Exception {
        String q = newQueue("update");
        String text = (String) op("broker", "updateQueue", q, "ANYCAST", 7, true);
        check(text.contains("maxConsumers=7") && text.contains("purgeOnNoConsumers=true"), text);
        eq(7L, longAttr(queue(q), "MaxConsumers"), "MaxConsumers");
        eq(true, attr(queue(q), "PurgeOnNoConsumers"), "PurgeOnNoConsumers");
        String json = (String) op("broker", "updateQueue", "{\"name\":\"" + q + "\",\"max-consumers\":-1,\"exclusive\":true}");
        eq(-1, JsonUtil.readJsonObject(json).getInt("max-consumers"), "max-consumers");
        eq(true, attr(queue(q), "Exclusive"), "Exclusive");
        String e = opFails("broker", "updateQueue", "nope." + RUN, "ANYCAST", 1, false);
        eq("AMQ229017: Queue nope." + RUN + " does not exist", e, "missing");
    }

    static void t_destroy_queue_with_consumer() throws Exception {
        String q = newQueue("consumed");
        try (ClientConsumer c = session.createConsumer(q)) {
            String e = opFails("broker", "destroyQueue", q);
            check(e.startsWith("AMQ229025"), "error " + e);
        }
        op("broker", "destroyQueue", q, false);
    }

    static void t_queue_counters() throws Exception {
        String q = newQueue("counters");
        send(q, 5);
        eq(5L, count(q), "MessageCount");
        eq(5L, longAttr(queue(q), "MessagesAdded"), "MessagesAdded");
        eq(5L, longAttr(queue(q), "DurableMessageCount"), "DurableMessageCount");
        check(longAttr(queue(q), "PersistentSize") > 0, "PersistentSize");
        ClientSession s = factory.createSession(false, false, false);
        s.start();
        try (ClientConsumer c = s.createConsumer(q)) {
            for (int i = 0; i < 2; i++) c.receive(2000).acknowledge();
            s.commit();
            ClientMessage m = c.receive(2000);
            check(m != null, "third");
            // Delivered, not acknowledged: the rest of the window is delivering too.
            check(longAttr(queue(q), "DeliveringCount") >= 1, "DeliveringCount");
            eq(1L, longAttr(queue(q), "ConsumerCount"), "ConsumerCount");
            String consumers = (String) op(queue(q), "listConsumersAsJSON");
            eq(1, JsonUtil.readJsonArray(consumers).size(), "consumers");
        }
        s.close();
        eq(2L, longAttr(queue(q), "MessagesAcknowledged"), "MessagesAcknowledged");
        eq(3L, count(q), "MessageCount after");
        eq(0L, longAttr(queue(q), "DeliveringCount"), "DeliveringCount after close");
        op(queue(q), "resetMessagesAdded");
        eq(0L, longAttr(queue(q), "MessagesAdded"), "reset");
    }

    static void t_count_messages() throws Exception {
        String q = newQueue("count");
        send(q, 3, Map.of("color", "red"));
        send(q, 2, Map.of("color", "blue"));
        eq(5L, ((Number) op(queue(q), "countMessages")).longValue(), "countMessages()");
        eq(3L, ((Number) op(queue(q), "countMessages", "color = 'red'")).longValue(), "countMessages(filter)");
        JsonObject groups = JsonUtil.readJsonObject((String) op(queue(q), "countMessages", null, "color"));
        eq(3, groups.getInt("red"), "red");
        eq(2, groups.getInt("blue"), "blue");
    }

    static void t_list_messages() throws Exception {
        String q = newQueue("list");
        send(q, 3, Map.of("color", "green"));
        JsonArray all = JsonUtil.readJsonArray((String) op(queue(q), "listMessagesAsJSON", (String) null));
        eq(3, all.size(), "all");
        JsonObject first = all.getJsonObject(0);
        eq("green", first.getString("color"), "property");
        eq(q, first.getString("address"), "address");
        eq(true, first.getBoolean("durable"), "durable");
        eq(0, first.getInt("i"), "first i");
        check(first.getJsonNumber("messageID").longValue() > 0, "messageID");
        JsonArray some = JsonUtil.readJsonArray((String) op(queue(q), "listMessagesAsJSON", "i > 0"));
        eq(2, some.size(), "filtered");
        Object[] maps = (Object[]) op(queue(q), "listMessages", "i = 2");
        eq(1, maps.length, "listMessages");
        JsonObject peek = JsonUtil.readJsonObject((String) op(queue(q), "peekFirstMessageAsJSON"));
        eq(0, peek.getInt("i"), "peek");
        // The broker's clock against the client's: only present.
        check(attr(queue(q), "FirstMessageAge") != null, "FirstMessageAge");
    }

    static void t_remove_messages() throws Exception {
        String q = newQueue("remove");
        send(q, 6);
        eq(2, ((Number) op(queue(q), "removeMessages", "i < 2")).intValue(), "removeMessages(filter)");
        eq(4L, count(q), "after filter");
        eq(2L, longAttr(queue(q), "MessagesKilled"), "MessagesKilled");
        JsonArray left = JsonUtil.readJsonArray((String) op(queue(q), "listMessagesAsJSON", "i = 2"));
        long id = left.getJsonObject(0).getJsonNumber("messageID").longValue();
        eq(true, op(queue(q), "removeMessage", id), "removeMessage");
        eq(false, op(queue(q), "removeMessage", id), "removeMessage again");
        eq(3L, count(q), "after removeMessage");
        eq(3, ((Number) op(queue(q), "removeAllMessages")).intValue(), "removeAllMessages");
        eq(0L, count(q), "empty");
    }

    static void t_move_messages() throws Exception {
        String from = newQueue("from");
        String to = newQueue("to");
        send(from, 4);
        eq(2, ((Number) op(queue(from), "moveMessages", "i >= 2", to)).intValue(), "moveMessages");
        waitCount(to, 2);
        eq(2L, count(from), "left");
        List<ClientMessage> moved = receive(to, 2);
        eq(from, moved.get(0).getStringProperty("_AMQ_ORIG_QUEUE"), "_AMQ_ORIG_QUEUE");
        eq(from, moved.get(0).getStringProperty("_AMQ_ORIG_ADDRESS"), "_AMQ_ORIG_ADDRESS");
        eq("Cannot move messages onto itself", opFails(queue(from), "moveMessages", null, from), "itself");
        eq("AMQ229049: No queue found for nope." + RUN, opFails(queue(from), "moveMessages", null, "nope." + RUN), "missing");
        JsonArray left = JsonUtil.readJsonArray((String) op(queue(from), "listMessagesAsJSON", (String) null));
        long id = left.getJsonObject(0).getJsonNumber("messageID").longValue();
        eq(true, op(queue(from), "moveMessage", id, to), "moveMessage");
        waitCount(to, 1);
        eq(1, ((Number) op(queue(from), "moveMessages", 100, null, to, false, 1)).intValue(), "moveMessages with count");
        waitCount(to, 2);
    }

    static void t_copy_message() throws Exception {
        String from = newQueue("copyfrom");
        String to = newQueue("copyto");
        send(from, 1);
        JsonArray left = JsonUtil.readJsonArray((String) op(queue(from), "listMessagesAsJSON", (String) null));
        long id = left.getJsonObject(0).getJsonNumber("messageID").longValue();
        eq(true, op(queue(from), "copyMessage", id, to), "copyMessage");
        waitCount(to, 1);
        eq(1L, count(from), "original stays");
    }

    static void t_dead_letter_and_retry() throws Exception {
        String q = newQueue("dla");
        send(q, 3);
        long dlq = count("DLQ");
        eq(2, ((Number) op(queue(q), "sendMessagesToDeadLetterAddress", "i < 2")).intValue(), "sendMessagesToDeadLetterAddress");
        waitCount("DLQ", dlq + 2);
        eq(1L, count(q), "left");
        int retried = ((Number) op(queue("DLQ"), "retryMessages", "_AMQ_ORIG_QUEUE = '" + q + "'")).intValue();
        eq(2, retried, "retryMessages");
        waitCount(q, 3);
        waitCount("DLQ", dlq);
    }

    static void t_expire_messages() throws Exception {
        String q = newQueue("expire");
        send(q, 3);
        long expiry = count("ExpiryQueue");
        eq(2, ((Number) op(queue(q), "expireMessages", "i > 0")).intValue(), "expireMessages");
        waitCount("ExpiryQueue", expiry + 2);
        eq(2L, longAttr(queue(q), "MessagesExpired"), "MessagesExpired");
        eq(1L, count(q), "left");
        op(queue("ExpiryQueue"), "removeMessages", "_AMQ_ORIG_QUEUE = '" + q + "'");
    }

    static void t_change_priority() throws Exception {
        String q = newQueue("priority");
        send(q, 3);
        eq(1, ((Number) op(queue(q), "changeMessagesPriority", "i = 2", 9)).intValue(), "changeMessagesPriority");
        // The message keeps its place; only its priority changes.
        List<ClientMessage> got = receive(q, 3);
        eq(2, got.get(2).getIntProperty("i"), "third");
        eq((byte) 9, got.get(2).getPriority(), "priority");
        String e = opFails(queue(q), "changeMessagesPriority", null, 10);
        eq("AMQ229048: invalid new Priority value: 10. It must be between 0 and 9 (both included)", e, "invalid");
    }

    static void t_pause_resume() throws Exception {
        String q = newQueue("pause");
        op(queue(q), "pause");
        eq(true, attr(queue(q), "Paused"), "Paused");
        eq(false, attr(queue(q), "PersistedPause"), "PersistedPause");
        send(q, 1);
        try (ClientConsumer c = session.createConsumer(q)) {
            check(c.receive(500) == null, "delivered while paused");
            op(queue(q), "resume");
            ClientMessage m = c.receive(2000);
            check(m != null, "not delivered after resume");
            m.acknowledge();
        }
        op(queue(q), "pause", true);
        eq(true, attr(queue(q), "PersistedPause"), "PersistedPause");
        op(queue(q), "resume");
        eq(false, attr(queue(q), "PersistedPause"), "PersistedPause after resume");
    }

    static void t_scheduled_messages() throws Exception {
        String q = newQueue("scheduled");
        try (ClientProducer p = session.createProducer(q)) {
            ClientMessage m = session.createMessage(true);
            m.putLongProperty("_AMQ_SCHED_DELIVERY", System.currentTimeMillis() + 60_000);
            m.putStringProperty("which", "later");
            p.send(m);
        }
        eq(1L, longAttr(queue(q), "ScheduledCount"), "ScheduledCount");
        JsonArray s = JsonUtil.readJsonArray((String) op(queue(q), "listScheduledMessagesAsJSON"));
        eq("later", s.getJsonObject(0).getString("which"), "listScheduledMessagesAsJSON");
        op(queue(q), "deliverScheduledMessages", (String) null);
        List<ClientMessage> got = receive(q, 1);
        eq(1, got.size(), "delivered now");
    }

    static void t_queue_send_message() throws Exception {
        String q = newQueue("sendq");
        String body = Base64.getEncoder().encodeToString(new byte[] {1, 2, 3});
        op(queue(q), "sendMessage", Map.of("k", "v"), 4, body, false, null, null);
        ClientMessage m = receive(q, 1).get(0);
        eq("v", m.getStringProperty("k"), "header");
        byte[] b = new byte[m.getBodySize()];
        m.getBodyBuffer().readBytes(b);
        eq("[1, 2, 3]", Arrays.toString(b), "body");
    }

    static void t_list_queues() throws Exception {
        String q = newQueue("listq");
        send(q, 2);
        String json = (String) op("broker", "listQueues",
            "{\"field\":\"name\",\"operation\":\"CONTAINS\",\"value\":\"" + q + "\",\"sortField\":\"name\",\"sortOrder\":\"desc\"}", 1, 50);
        JsonObject o = JsonUtil.readJsonObject(json);
        eq(1, o.getInt("count"), "count");
        JsonObject first = o.getJsonArray("data").getJsonObject(0);
        eq(q, first.getString("name"), "name");
        eq("2", first.getString("messageCount"), "messageCount");
        eq("ANYCAST", first.getString("routingType"), "routingType");
        eq("false", first.getString("paused"), "paused");
        String gt = (String) op("broker", "listQueues",
            "{\"field\":\"MESSAGE_COUNT\",\"operation\":\"GREATER_THAN\",\"value\":\"1\"}", 1, 1000);
        boolean found = false;
        for (var v : JsonUtil.readJsonObject(gt).getJsonArray("data")) {
            found |= ((JsonObject) v).getString("name").equals(q);
        }
        check(found, "GREATER_THAN");
        Object[] names = (Object[]) op("broker", "getQueueNames", "ANYCAST");
        check(Arrays.asList(names).contains(q), "getQueueNames(ANYCAST)");
    }

    // ------------------------------------------------------------ diverts and settings

    static void t_divert() throws Exception {
        String a = newQueue("divertsource");
        String target = newQueue("diverttarget");
        String d = name("divert");
        op("broker", "createDivert", d, d, a, target, false, null, null, "ANYCAST");
        check(Arrays.asList((Object[]) attr("broker", "DivertNames")).contains(d), "DivertNames");
        eq(target, attr("divert." + d, "ForwardingAddress"), "ForwardingAddress");
        send(a, 1);
        waitCount(target, 1);
        eq(1L, count(a), "copy and original");
        op("broker", "destroyDivert", d);
        eq("AMQ229012: No binding for divert " + d, opFails("broker", "destroyDivert", d), "gone");
    }

    static void t_address_settings() throws Exception {
        String m = name("settings") + ".#";
        String result = (String) op("broker", "addAddressSettings", m,
            "{\"maxDeliveryAttempts\":7,\"deadLetterAddress\":\"OtherDLQ\",\"addressFullMessagePolicy\":\"BLOCK\"}");
        JsonObject added = JsonUtil.readJsonObject(result);
        eq(7, added.getInt("maxDeliveryAttempts"), "added");
        String address = m.substring(0, m.length() - 2) + ".x";
        JsonObject s = JsonUtil.readJsonObject((String) op("broker", "getAddressSettingsAsJSON", address));
        eq(7, s.getInt("maxDeliveryAttempts"), "maxDeliveryAttempts");
        eq("OtherDLQ", s.getString("deadLetterAddress"), "deadLetterAddress");
        eq("BLOCK", s.getString("addressFullMessagePolicy"), "addressFullMessagePolicy");
        session.createQueue(QueueConfiguration.of(address).setRoutingType(RoutingType.ANYCAST));
        eq("OtherDLQ", attr(queue(address), "DeadLetterAddress"), "queue sees it");
        op("broker", "removeAddressSettings", m);
        JsonObject after = JsonUtil.readJsonObject((String) op("broker", "getAddressSettingsAsJSON", address));
        check(!after.containsKey("maxDeliveryAttempts"), "removed: " + after);
        eq("DLQ", attr(queue(address), "DeadLetterAddress"), "back to the configured");
    }

    // ------------------------------------------------------------ other protocols

    static void t_jms_management() throws Exception {
        String q = newQueue("jms");
        send(q, 2);
        ConnectionFactory cf = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(url);
        try (Connection c = cf.createConnection()) {
            c.start();
            QueueSession s = ((QueueConnection) c).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue management = s.createQueue("activemq.management");
            QueueRequestor r = new QueueRequestor(s, management);
            Message m = s.createMessage();
            JMSManagementHelper.putAttribute(m, queue(q), "MessageCount");
            Message reply = r.request(m);
            check(JMSManagementHelper.hasOperationSucceeded(reply), "attribute");
            eq(2L, ((Number) JMSManagementHelper.getResult(reply)).longValue(), "MessageCount");
            m = s.createMessage();
            JMSManagementHelper.putOperationInvocation(m, queue(q), "countMessages", "i = 1");
            reply = r.request(m);
            check(JMSManagementHelper.hasOperationSucceeded(reply), "operation");
            eq(1L, ((Number) JMSManagementHelper.getResult(reply)).longValue(), "countMessages");
        }
    }

    static void t_amqp_management() throws Exception {
        String q = newQueue("amqp");
        send(q, 3);
        ConnectionFactory cf = new org.apache.qpid.jms.JmsConnectionFactory("amqp://" + host + ":" + port);
        try (Connection c = cf.createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue replies = s.createTemporaryQueue();
            Message m = s.createMessage();
            m.setStringProperty("_AMQ_ResourceName", queue(q));
            m.setStringProperty("_AMQ_Attribute", "MessageCount");
            m.setJMSReplyTo(replies);
            s.createProducer(s.createQueue("activemq.management")).send(m);
            Message reply = s.createConsumer(replies).receive(5000);
            check(reply != null, "no reply");
            eq(true, reply.getBooleanProperty("_AMQ_OperationSucceeded"), "succeeded");
            eq("[3]", ((TextMessage) reply).getText(), "result");
        }
    }
}
