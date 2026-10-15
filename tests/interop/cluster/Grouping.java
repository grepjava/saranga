// The clients of the grouping suite, all with the Artemis JMS client (Core):
//   java -cp '<artemis>/lib/*' Grouping.java redistribute <url1> <url2> <queue> <count>
//       messages sent to node 1 while no node has a consumer, then a
//       consumer on node 2: node 1 redistributes them all to it, in order.
//   java -cp '<artemis>/lib/*' Grouping.java grouped <url1> <url2> <queue> <count> <groups> <node> <map>
//       a consumer on each node, then messages of groups g0..g<groups-1>
//       sent to node <node>: each group goes to one consumer, in order.
//       The node each group went to is written to <map>, or, when <map>
//       exists, compared with what it holds.
// tests/interop/cluster/grouping.sh runs it; it prints what it saw.

import java.nio.file.*;
import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Grouping {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a consumer's creation takes to reach the other node. */
    static final long PROPAGATION = 2000;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "redistribute" -> redistribute(args[1], args[2], args[3], Integer.parseInt(args[4]));
            case "grouped" -> grouped(args[1], args[2], args[3], Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), Path.of(args[7]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static void redistribute(String url1, String url2, String queue, int count) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s1.createProducer(s1.createQueue(queue));
            for (int i = 0; i < count; i++) {
                p.send(s1.createTextMessage("m" + i));
            }
            System.out.println("sent " + count + " to node 1");
            Thread.sleep(PROPAGATION);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s2.createConsumer(s2.createQueue(queue));
            c2.start();
            List<String> got = new ArrayList<>();
            while (got.size() < count) {
                Message m = consumer.receive(10000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
            }
            System.out.println("node 2 received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
        }
    }

    static void grouped(String url1, String url2, String queue, int count, int groups, int node, Path map)
        throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            List<MessageConsumer> consumers = List.of(
                s1.createConsumer(s1.createQueue(queue)), s2.createConsumer(s2.createQueue(queue)));
            c1.start();
            c2.start();
            Thread.sleep(PROPAGATION);
            Session s = node == 1 ? s1 : s2;
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (int i = 0; i < count; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setStringProperty("JMSXGroupID", "g" + (i % groups));
                m.setIntProperty("seq", i);
                p.send(m);
            }
            System.out.println("sent " + count + " in " + groups + " groups to node " + node);
            // The node each group's messages came to, and their order.
            Map<String, Set<Integer>> nodes = new TreeMap<>();
            Map<String, List<Integer>> seqs = new TreeMap<>();
            int received = 0;
            long deadline = System.currentTimeMillis() + 15000;
            while (received < count && System.currentTimeMillis() < deadline) {
                for (int n = 0; n < 2; n++) {
                    Message m = consumers.get(n).receive(100);
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
            if (Files.exists(map)) {
                System.out.println("same nodes as before: " + Files.readString(map).equals(now.toString()));
            } else {
                Files.writeString(map, now.toString());
            }
        }
    }
}
