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
// tests/interop/cluster/cluster.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.client.TopologyMember;
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
            default -> throw new IllegalArgumentException(args[0]);
        }
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
