// The clients of the cluster suite, all with the Artemis JMS client (Core):
//   java -cp '<artemis>/lib/*' Cluster.java topology <url>
//       how many nodes the topology a client is given holds, each with a
//       primary.
//   java -cp '<artemis>/lib/*' Cluster.java on-demand <url1> <url2> <queue> <count>
//       a consumer on node 2, then messages sent to node 1: all of them
//       reach the consumer, in order.
//   java -cp '<artemis>/lib/*' Cluster.java balance <url1> <url2> <queue> <count>
//       a consumer on each node, then messages sent to node 1: how many
//       each consumer received.
//   java -cp '<artemis>/lib/*' Cluster.java topic <url1> <url2> <topic> <count>
//       a subscriber on each node, then messages published on node 1: each
//       subscriber receives them all.
//   java -cp '<artemis>/lib/*' Cluster.java send <url> <queue> <count>
//   java -cp '<artemis>/lib/*' Cluster.java receive <url> <queue> <count>
//       the messages, then that no more come.
//   java -cp '<artemis>/lib/*' Cluster.java control <url>
//       ClusterConnectionControl's attributes and getBridgeMetrics, through
//       management messages, with what differs between runs left out.
//   java -cp '<artemis>/lib/*' Cluster.java control-restart <url>
//       stop through management, then start, and the bridge comes back.
//   java -cp '<artemis>/lib/*' Cluster.java controls <url>
//       every cluster connection's control, and the network topology.
//   java -cp '<artemis>/lib/*' Cluster.java drain <url> <queue>
//       how many messages the queue has, taking them.
// tests/interop/cluster/cluster.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Cluster {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a consumer's creation takes to reach the other node. */
    static final long PROPAGATION = 2000;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "topology" -> topology(args[1]);
            case "on-demand" -> onDemand(args[1], args[2], args[3], Integer.parseInt(args[4]));
            case "balance" -> balance(args[1], args[2], args[3], Integer.parseInt(args[4]));
            case "topic" -> topic(args[1], args[2], args[3], Integer.parseInt(args[4]));
            case "send" -> send(args[1], args[2], Integer.parseInt(args[3]));
            case "receive" -> receive(args[1], args[2], Integer.parseInt(args[3]));
            case "control" -> control(args[1]);
            case "control-restart" -> controlRestart(args[1]);
            case "controls" -> controls(args[1]);
            case "drain" -> drain(args[1], args[2]);
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    /** A management call: an attribute (no operation) or an operation. */
    static Object manage(String url, String resource, String name, boolean operation, Object... params)
            throws Exception {
        try (QueueConnection c = new ActiveMQConnectionFactory(url).createQueueConnection(USER, PASSWORD)) {
            QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueRequestor r = new QueueRequestor(s, s.createQueue("activemq.management"));
            c.start();
            Message m = s.createMessage();
            if (operation) {
                JMSManagementHelper.putOperationInvocation(m, resource, name, params);
            } else {
                JMSManagementHelper.putAttribute(m, resource, name);
            }
            Message reply = r.request(m);
            Object result = JMSManagementHelper.getResult(reply);
            if (!JMSManagementHelper.hasOperationSucceeded(reply)) {
                throw new java.lang.IllegalStateException(String.valueOf(result));
            }
            return result;
        }
    }

    static String show(Object v) {
        if (v instanceof Object[] a) {
            return Arrays.toString(a);
        }
        if (v instanceof Map<?, ?> m) {
            return new TreeMap<>(m).toString();
        }
        return String.valueOf(v);
    }

    /** The cluster connection's name, from the broker's list. */
    static String clusterConnection(String url) throws Exception {
        Object[] names = (Object[]) manage(url, "broker", "clusterConnectionNames", false);
        System.out.println("clusterConnectionNames = " + Arrays.toString(names));
        return "clusterconnection." + names[0];
    }

    /** The nodes bridged to, waiting up to 10 s for one. */
    static Map<?, ?> nodes(String url, String resource) throws Exception {
        Map<?, ?> nodes = Map.of();
        for (int i = 0; i < 100 && nodes.isEmpty(); i++) {
            nodes = (Map<?, ?>) manage(url, resource, "nodes", false);
            if (nodes.isEmpty()) {
                Thread.sleep(100);
            }
        }
        return nodes;
    }

    static void control(String url) throws Exception {
        String cc = clusterConnection(url);
        for (String a : new String[] {"name", "address", "duplicateDetection", "messageLoadBalancingType",
                "maxHops", "staticConnectors", "staticConnectorsAsJSON", "discoveryGroupName",
                "retryInterval", "producerWindowSize", "started", "messagesPendingAcknowledgement"}) {
            System.out.println(a + " = " + show(manage(url, cc, a, false)));
        }
        String nodeId = String.valueOf(manage(url, cc, "nodeID", false));
        System.out.println("nodeID is a UUID: " + nodeId.matches("[0-9a-f-]{36}"));
        Map<?, ?> nodes = nodes(url, cc);
        System.out.println("nodes: " + nodes.size() + ", the other node's address "
            + nodes.values().stream().map(String::valueOf).toList());
        String other = String.valueOf(nodes.keySet().iterator().next());
        System.out.println("nodes leave out this node: " + !nodes.containsKey(nodeId));
        String topology = String.valueOf(manage(url, cc, "topology", false));
        System.out.println("topology: starts " + topology.startsWith("topology on Topology@")
            + ", members " + topology.lines().filter(l -> l.contains(" => TopologyMember[id=")).count()
            + ", " + topology.substring(topology.lastIndexOf("nodes=")).replace('\t', ' '));
        Map<?, ?> metrics = (Map<?, ?>) manage(url, cc, "metrics", false);
        System.out.println("metrics keys " + new TreeSet<>(metrics.keySet()));
        Map<?, ?> bridge = (Map<?, ?>) manage(url, cc, "getBridgeMetrics", true, other);
        System.out.println("getBridgeMetrics(other) keys " + new TreeSet<>(bridge.keySet())
            + ", pending " + bridge.get("messagesPendingAcknowledgement"));
        System.out.println("getBridgeMetrics(unknown) = "
            + manage(url, cc, "getBridgeMetrics", true, "00000000-0000-0000-0000-000000000000"));
    }

    static void controls(String url) throws Exception {
        Object[] names = (Object[]) manage(url, "broker", "clusterConnectionNames", false);
        List<String> sorted = new ArrayList<>();
        for (Object n : names) {
            sorted.add(String.valueOf(n));
        }
        Collections.sort(sorted);
        System.out.println("clusterConnectionNames (sorted) = " + sorted);
        System.out.println("clustered = " + manage(url, "broker", "clustered", false));
        for (String name : sorted) {
            String cc = "clusterconnection." + name;
            StringBuilder b = new StringBuilder(name + ":");
            for (String a : new String[] {"address", "messageLoadBalancingType", "staticConnectors",
                    "discoveryGroupName", "started"}) {
                b.append(' ').append(a).append('=').append(show(manage(url, cc, a, false)));
            }
            b.append(" nodes=").append(nodes(url, cc).size());
            System.out.println(b);
        }
        String topology = String.valueOf(manage(url, "broker", "listNetworkTopology", true));
        System.out.println("listNetworkTopology entries = " + (topology.split("\"nodeID\"", -1).length - 1));
    }

    static void drain(String url, String queue) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            int n = 0;
            while (consumer.receive(2000) != null) {
                n++;
            }
            System.out.println("drained " + n);
        }
    }

    static void controlRestart(String url) throws Exception {
        String cc = clusterConnection(url);
        manage(url, cc, "stop", true);
        System.out.println("stopped: started = " + manage(url, cc, "started", false)
            + ", nodes = " + show(manage(url, cc, "nodes", false)));
        manage(url, cc, "start", true);
        System.out.println("started: started = " + manage(url, cc, "started", false)
            + ", nodes " + nodes(url, cc).size());
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static void topology(String url) throws Exception {
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?ha=true");
             Connection c = cf.createConnection(USER, PASSWORD)) {
            Collection<? extends TopologyMember> members = List.of();
            for (int i = 0; i < 100; i++) {
                members = cf.getServerLocator().getTopology().getMembers();
                if (members.size() >= 2) {
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

    static void send(String url, String queue, int count) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            send(s, s.createQueue(queue), count);
            System.out.println("sent " + count);
        }
    }

    /** The texts of up to count messages, waiting 5 s for each. */
    static List<String> take(MessageConsumer consumer, int count, long wait) throws JMSException {
        List<String> got = new ArrayList<>();
        while (got.size() < count) {
            Message m = consumer.receive(wait);
            if (m == null) {
                break;
            }
            got.add(((TextMessage) m).getText());
        }
        return got;
    }

    static void receive(String url, String queue, int count) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            List<String> got = take(consumer, count, 5000);
            System.out.println("received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
        }
    }

    static void onDemand(String url1, String url2, String queue, int count) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s2.createConsumer(s2.createQueue(queue));
            c2.start();
            Thread.sleep(PROPAGATION);
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            send(s1, s1.createQueue(queue), count);
            List<String> got = take(consumer, count, 5000);
            System.out.println("node 2 received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
        }
    }

    static void balance(String url1, String url2, String queue, int count) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer k1 = s1.createConsumer(s1.createQueue(queue));
            MessageConsumer k2 = s2.createConsumer(s2.createQueue(queue));
            c1.start();
            c2.start();
            Thread.sleep(PROPAGATION);
            send(s1, s1.createQueue(queue), count);
            int n1 = 0, n2 = 0;
            for (int i = 0; i < count; i++) {
                if (k1.receive(200) != null) {
                    n1++;
                }
                if (k2.receive(200) != null) {
                    n2++;
                }
                if (n1 + n2 == count) {
                    break;
                }
            }
            System.out.println("node 1 received " + n1 + ", node 2 received " + n2);
        }
    }

    static void topic(String url1, String url2, String topic, int count) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer k1 = s1.createConsumer(s1.createTopic(topic));
            MessageConsumer k2 = s2.createConsumer(s2.createTopic(topic));
            c1.start();
            c2.start();
            Thread.sleep(PROPAGATION);
            send(s1, s1.createTopic(topic), count);
            System.out.println("node 1 subscriber received " + take(k1, count, 5000));
            System.out.println("node 2 subscriber received " + take(k2, count, 5000));
        }
    }
}
