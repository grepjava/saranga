// The clients of the federation suite, all with the Artemis JMS client (Core):
//   java -cp '<artemis>/lib/*' Federation.java topic <url1> <url2> <topic> <count> <client-id>
//       a durable subscriber on node 2, then messages published on node 1:
//       node 2 federates them from node 1, each with one hop. Prints the
//       distinct messages received.
//   java -cp '<artemis>/lib/*' Federation.java queue <url1> <url2> <queue> <count>
//       messages sent to the queue on node 1, then a consumer on node 2:
//       node 2 federates them from node 1, and node 1 is left with none.
// tests/interop/cluster/federation.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Federation {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a federated consumer takes to connect upstream. */
    static final long PROPAGATION = 3000;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "topic" -> topic(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
            case "queue" -> queue(args[1], args[2], args[3], Integer.parseInt(args[4]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static String hops(Message m) throws JMSException {
        Object h = m.getObjectProperty("_AMQ_Hops");
        return h instanceof byte[] b ? Arrays.toString(b) : String.valueOf(h);
    }

    static void topic(String url1, String url2, String topic, int count, String clientId) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            c2.setClientID(clientId);
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer subscriber = s2.createDurableSubscriber(s2.createTopic(topic), "sub");
            c2.start();
            Thread.sleep(PROPAGATION);
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s1.createProducer(s1.createTopic(topic));
            for (int i = 0; i < count; i++) {
                p.send(s1.createTextMessage("m" + i));
            }
            System.out.println("published " + count + " on node 1");
            // Federation is at least once: a federated consumer's batched
            // acknowledgements are lost when the upstream stops, and what
            // it took is delivered again.
            Set<String> got = new TreeSet<>();
            Set<String> hops = new TreeSet<>();
            while (got.size() < count) {
                Message m = subscriber.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
                hops.add(hops(m));
            }
            for (Message m; (m = subscriber.receive(1000)) != null; ) {
                got.add(((TextMessage) m).getText());
                hops.add(hops(m));
            }
            System.out.println("node 2 received " + got);
            System.out.println("hops " + hops);
        }
    }

    static void queue(String url1, String url2, String queue, int count) throws Exception {
        try (Connection c1 = core(url1); Connection c2 = core(url2)) {
            Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s1.createProducer(s1.createQueue(queue));
            for (int i = 0; i < count; i++) {
                p.send(s1.createTextMessage("m" + i));
            }
            System.out.println("sent " + count + " to node 1");
            Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s2.createConsumer(s2.createQueue(queue));
            c2.start();
            List<String> got = new ArrayList<>();
            while (got.size() < count) {
                Message m = consumer.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
            }
            System.out.println("node 2 received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
            consumer.close();
            Thread.sleep(1000);
            c1.start();
            MessageConsumer left = s1.createConsumer(s1.createQueue(queue));
            System.out.println(left.receive(1000) == null ? "node 1 has none left" : "node 1 has MORE");
        }
    }
}
