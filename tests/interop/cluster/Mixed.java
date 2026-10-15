// The clients of the mixed suite, all with the Artemis JMS client (Core).
// <urls> is a comma-separated list of the nodes, node 1 first.
//   java -cp '<artemis>/lib/*' Mixed.java topology <urls>
//       how many nodes the topology a client of node 1 is given holds,
//       each with a primary.
//   java -cp '<artemis>/lib/*' Mixed.java balance <urls> <queue> <count>
//       a consumer on each node, then messages sent to node 1: how many
//       each consumer received.
//   java -cp '<artemis>/lib/*' Mixed.java topic <urls> <topic> <count>
//       a subscriber on each node, then messages published on node 1:
//       whether each subscriber received them all, in order.
//   java -cp '<artemis>/lib/*' Mixed.java redistribute <urls> <queue> <count> <from> <to>
//       messages sent to node <from> while no node has a consumer, then a
//       consumer on node <to>: they are all redistributed to it, in order.
//   java -cp '<artemis>/lib/*' Mixed.java grouped <urls> <queue> <count> <groups> <node> <map> [<left>]
//       a consumer on each node, then messages of groups g0..g<groups-1>
//       sent to node <node>: each group goes to one consumer, in order.
//       The node each group went to is written to <map>, or, when <map>
//       exists, compared with what it holds. With <left>, a node that
//       stopped since, only the groups that were not on it are compared:
//       the groups of a node that leaves are forgotten.
//   java -cp '<artemis>/lib/*' Mixed.java load <urls> <queue> <count>
//       a transacted consumer on each node, and a producer on each node
//       sending <count> messages in transactions of ten: every message
//       is received once, wherever it was sent.
//   java -cp '<artemis>/lib/*' Mixed.java network-topology <urls> <node>
//       node <node>'s listNetworkTopology, through a management message:
//       the members' primaries, sorted, and whether each has its node ID
//       and "live" beside "primary".
//   java -cp '<artemis>/lib/*' Mixed.java mqtt-takeover <urls> <client-id> <from> <to>
//       an MQTT 3.1.1 client connected to node <from>, then one with the
//       same client ID to node <to>: node <from> closes the first.
// tests/interop/cluster/mixed.sh runs it; it prints what it saw.

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Mixed {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a consumer's creation takes to reach the other nodes. */
    static final long PROPAGATION = 2000;

    public static void main(String[] args) throws Exception {
        List<String> urls = List.of(args[1].split(","));
        switch (args[0]) {
            case "topology" -> topology(urls);
            case "balance" -> balance(urls, args[2], Integer.parseInt(args[3]));
            case "topic" -> topic(urls, args[2], Integer.parseInt(args[3]));
            case "redistribute" -> redistribute(urls, args[2], Integer.parseInt(args[3]),
                Integer.parseInt(args[4]), Integer.parseInt(args[5]));
            case "grouped" -> grouped(urls, args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Path.of(args[6]), args.length > 7 ? Integer.parseInt(args[7]) : 0);
            case "load" -> load(urls, args[2], Integer.parseInt(args[3]));
            case "network-topology" -> networkTopology(urls, Integer.parseInt(args[2]));
            case "mqtt-takeover" -> mqttTakeover(urls, args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static List<Connection> connect(List<String> urls) throws JMSException {
        List<Connection> cs = new ArrayList<>();
        for (String url : urls) {
            cs.add(core(url));
        }
        return cs;
    }

    static void close(List<Connection> cs) throws JMSException {
        for (Connection c : cs) {
            c.close();
        }
    }

    static void topology(List<String> urls) throws Exception {
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(urls.get(0) + "?ha=true");
             Connection c = cf.createConnection(USER, PASSWORD)) {
            Collection<? extends TopologyMember> members = List.of();
            for (int i = 0; i < 100; i++) {
                members = cf.getServerLocator().getTopology().getMembers();
                if (members.size() >= urls.size()) {
                    break;
                }
                Thread.sleep(100);
            }
            long primaries = members.stream().filter(m -> m.getPrimary() != null).count();
            System.out.println("topology: " + members.size() + " nodes, " + primaries + " with a primary");
        }
    }

    static void send(Session s, Destination d, int count) throws JMSException {
        MessageProducer p = s.createProducer(d);
        for (int i = 0; i < count; i++) {
            TextMessage m = s.createTextMessage("m" + i);
            m.setIntProperty("seq", i);
            p.send(m);
        }
        p.close();
    }

    static void balance(List<String> urls, String queue, int count) throws Exception {
        List<Connection> cs = connect(urls);
        try {
            List<Session> ss = new ArrayList<>();
            List<MessageConsumer> ks = new ArrayList<>();
            for (Connection c : cs) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                ss.add(s);
                ks.add(s.createConsumer(s.createQueue(queue)));
                c.start();
            }
            Thread.sleep(PROPAGATION);
            send(ss.get(0), ss.get(0).createQueue(queue), count);
            int[] got = new int[ks.size()];
            int total = 0;
            long deadline = System.currentTimeMillis() + 15000;
            while (total < count && System.currentTimeMillis() < deadline) {
                for (int n = 0; n < ks.size(); n++) {
                    if (ks.get(n).receive(100) != null) {
                        got[n]++;
                        total++;
                    }
                }
            }
            StringBuilder line = new StringBuilder();
            for (int n = 0; n < got.length; n++) {
                line.append(n == 0 ? "" : ", ").append("node ").append(n + 1).append(" received ").append(got[n]);
            }
            System.out.println(line);
        } finally {
            close(cs);
        }
    }

    static List<Integer> take(MessageConsumer consumer, int count, long wait) throws JMSException {
        List<Integer> got = new ArrayList<>();
        while (got.size() < count) {
            Message m = consumer.receive(wait);
            if (m == null) {
                break;
            }
            got.add(m.getIntProperty("seq"));
        }
        return got;
    }

    static void topic(List<String> urls, String topic, int count) throws Exception {
        List<Connection> cs = connect(urls);
        try {
            List<Session> ss = new ArrayList<>();
            List<MessageConsumer> ks = new ArrayList<>();
            for (Connection c : cs) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                ss.add(s);
                ks.add(s.createConsumer(s.createTopic(topic)));
                c.start();
            }
            Thread.sleep(PROPAGATION);
            send(ss.get(0), ss.get(0).createTopic(topic), count);
            for (int n = 0; n < ks.size(); n++) {
                List<Integer> got = take(ks.get(n), count, 5000);
                boolean inOrder = got.equals(got.stream().sorted().toList());
                System.out.println("node " + (n + 1) + " subscriber received " + got.size()
                    + (inOrder ? ", in order" : ", OUT OF ORDER"));
            }
        } finally {
            close(cs);
        }
    }

    static void redistribute(List<String> urls, String queue, int count, int from, int to) throws Exception {
        try (Connection c1 = core(urls.get(from - 1)); Connection c2 = core(urls.get(to - 1))) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            send(s1, s1.createQueue(queue), count);
            System.out.println("sent " + count + " to node " + from);
            Thread.sleep(PROPAGATION);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s2.createConsumer(s2.createQueue(queue));
            c2.start();
            List<Integer> got = take(consumer, count, 10000);
            System.out.println("node " + to + " received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
        }
    }

    static void grouped(List<String> urls, String queue, int count, int groups, int node, Path map, int left)
        throws Exception {
        List<Connection> cs = connect(urls);
        try {
            List<Session> ss = new ArrayList<>();
            List<MessageConsumer> ks = new ArrayList<>();
            for (Connection c : cs) {
                Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
                ss.add(s);
                ks.add(s.createConsumer(s.createQueue(queue)));
                c.start();
            }
            Thread.sleep(PROPAGATION);
            Session s = ss.get(node - 1);
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (int i = 0; i < count; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setStringProperty("JMSXGroupID", "g" + (i % groups));
                m.setIntProperty("seq", i);
                p.send(m);
            }
            System.out.println("sent " + count + " in " + groups + " groups to node " + node);
            Map<String, Set<Integer>> nodes = new TreeMap<>();
            Map<String, List<Integer>> seqs = new TreeMap<>();
            int received = 0;
            long deadline = System.currentTimeMillis() + 20000;
            while (received < count && System.currentTimeMillis() < deadline) {
                for (int n = 0; n < ks.size(); n++) {
                    Message m = ks.get(n).receive(100);
                    if (m != null) {
                        String g = m.getStringProperty("JMSXGroupID");
                        nodes.computeIfAbsent(g, k -> new TreeSet<>()).add(n + 1);
                        seqs.computeIfAbsent(g, k -> new ArrayList<>()).add(m.getIntProperty("seq"));
                        received++;
                    }
                }
            }
            System.out.println("received " + received);
            System.out.println("groups: " + nodes.size());
            System.out.println("each group to one node: " + nodes.values().stream().allMatch(v -> v.size() == 1));
            System.out.println("each group in order: " + seqs.values().stream().allMatch(
                l -> l.equals(l.stream().sorted().toList())));
            StringBuilder now = new StringBuilder();
            nodes.forEach((g, v) -> now.append(g).append('=').append(v).append('\n'));
            if (left != 0) {
                String gone = "=[" + left + "]";
                List<String> before = Files.readAllLines(map).stream().filter(l -> !l.endsWith(gone)).toList();
                List<String> after = List.of(now.toString().split("\n"));
                System.out.println("same nodes as before, but node " + left + "'s groups: " + after.containsAll(before));
            } else if (Files.exists(map)) {
                System.out.println("same nodes as before: " + Files.readString(map).equals(now.toString()));
            } else {
                Files.writeString(map, now.toString());
            }
        } finally {
            close(cs);
        }
    }

    static void load(List<String> urls, String queue, int count) throws Exception {
        int nodes = urls.size();
        int total = nodes * count;
        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger earlier = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        // Messages of an earlier run, left on a node, are told apart.
        String run = Long.toString(System.nanoTime(), 36);
        List<Connection> cs = connect(urls);
        try {
            List<Thread> consumers = new ArrayList<>();
            for (Connection c : cs) {
                Session s = c.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer k = s.createConsumer(s.createQueue(queue));
                c.start();
                Thread t = new Thread(() -> {
                    try {
                        List<String> batch = new ArrayList<>();
                        while (!stop.get()) {
                            Message m = k.receive(200);
                            if (m != null) {
                                if (run.equals(m.getStringProperty("run"))) {
                                    batch.add(((TextMessage) m).getText());
                                } else {
                                    batch.add(null);
                                }
                            }
                            if (batch.size() == 10 || (m == null && !batch.isEmpty())) {
                                s.commit();
                                for (String id : batch) {
                                    if (id == null) {
                                        earlier.incrementAndGet();
                                    } else if (!seen.add(id)) {
                                        duplicates.incrementAndGet();
                                    }
                                }
                                batch.clear();
                            }
                        }
                    } catch (JMSException e) {
                        System.out.println("consumer failed: " + e);
                    }
                });
                t.start();
                consumers.add(t);
            }
            Thread.sleep(PROPAGATION);
            List<Thread> producers = new ArrayList<>();
            for (int n = 0; n < nodes; n++) {
                Connection c = cs.get(n);
                int node = n + 1;
                Thread t = new Thread(() -> {
                    try (Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
                        MessageProducer p = s.createProducer(s.createQueue(queue));
                        for (int i = 0; i < count; i++) {
                            TextMessage m = s.createTextMessage("n" + node + "-" + i);
                            m.setStringProperty("run", run);
                            p.send(m);
                            if (i % 10 == 9) {
                                s.commit();
                            }
                        }
                        s.commit();
                    } catch (JMSException e) {
                        System.out.println("producer failed: " + e);
                    }
                });
                t.start();
                producers.add(t);
            }
            for (Thread t : producers) {
                t.join();
            }
            long deadline = System.currentTimeMillis() + 60000;
            while (seen.size() < total && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            Thread.sleep(1000);
            stop.set(true);
            for (Thread t : consumers) {
                t.join();
            }
            int missing = 0;
            for (int n = 1; n <= nodes; n++) {
                for (int i = 0; i < count; i++) {
                    if (!seen.contains("n" + n + "-" + i)) {
                        missing++;
                    }
                }
            }
            System.out.println("sent " + total + " on " + nodes + " nodes, received " + seen.size()
                + ", missing " + missing + ", duplicates " + duplicates.get());
            if (earlier.get() > 0) {
                System.err.println("(" + earlier.get() + " messages of earlier runs)");
            }
        } finally {
            close(cs);
        }
    }

    /** A connection that sent an MQTT 3.1.1 CONNECT (clean session, the user and password) and was accepted. */
    static Socket mqttConnect(String url, String clientId) throws IOException {
        URI uri = URI.create(url);
        Socket socket = new Socket(uri.getHost(), uri.getPort());
        socket.setSoTimeout(10000);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        mqttString(body, "MQTT");
        body.write(4);
        body.write(0xC2);
        body.write(0);
        body.write(60);
        mqttString(body, clientId);
        mqttString(body, USER);
        mqttString(body, PASSWORD);
        OutputStream out = socket.getOutputStream();
        out.write(0x10);
        out.write(body.size());
        body.writeTo(out);
        out.flush();
        InputStream in = socket.getInputStream();
        byte[] connack = in.readNBytes(4);
        if (connack.length != 4 || connack[0] != 0x20 || connack[3] != 0) {
            throw new IOException("not accepted: " + Arrays.toString(connack));
        }
        return socket;
    }

    static void mqttString(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.write(b.length >> 8);
        out.write(b.length & 0xff);
        out.write(b, 0, b.length);
    }

    static void mqttTakeover(List<String> urls, String clientId, int from, int to) throws Exception {
        try (Socket first = mqttConnect(urls.get(from - 1), clientId)) {
            System.out.println("connected " + clientId + " to node " + from);
            Thread.sleep(PROPAGATION);
            try (Socket second = mqttConnect(urls.get(to - 1), clientId)) {
                System.out.println("connected " + clientId + " to node " + to);
                boolean closed;
                try {
                    closed = first.getInputStream().read() == -1;
                } catch (SocketTimeoutException e) {
                    closed = false;
                } catch (IOException e) {
                    closed = true;
                }
                System.out.println("node " + from + " closed the first connection: " + closed);
            }
        }
    }

    static void networkTopology(List<String> urls, int node) throws Exception {
        try (Connection c = core(urls.get(node - 1))) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueRequestor requestor = new QueueRequestor((QueueSession) s, s.createQueue("activemq.management"));
            c.start();
            Message m = s.createMessage();
            JMSManagementHelper.putOperationInvocation(m, "broker", "listNetworkTopology");
            Message reply = requestor.request(m);
            String json = (String) JMSManagementHelper.getResult(reply, String.class);
            JsonArray members = JsonUtil.readJsonArray(json);
            List<String> primaries = new ArrayList<>();
            boolean complete = true;
            for (int i = 0; i < members.size(); i++) {
                JsonObject o = members.getJsonObject(i);
                primaries.add(o.getString("primary"));
                complete &= !o.getString("nodeID").isEmpty() && o.getString("live").equals(o.getString("primary"))
                    && List.copyOf(o.keySet()).subList(0, 3).equals(List.of("nodeID", "live", "primary"));
            }
            Collections.sort(primaries);
            System.out.println("node " + node + " network topology: " + primaries + (complete ? "" : " INCOMPLETE"));
        }
    }
}
