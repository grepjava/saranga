// The clients of the AMQP federation suite, with the Artemis JMS client
// (Core):
//   java -cp '<artemis>/lib/*' AmqpFederation.java topic <sub-url> <pub-url> <topic> <count> <client-id>
//       a durable subscriber on one node, then messages published on the
//       other: the subscriber's node federates them, each with one hop.
//       Prints the distinct messages received and their hops.
//   java -cp '<artemis>/lib/*' AmqpFederation.java queue <send-url> <receive-url> <queue> <count>
//       messages sent to the queue on one node, then a consumer on the
//       other: its node federates them, and the first is left with none.
//   java -cp '<artemis>/lib/*' AmqpFederation.java late <send-url> <receive-url> <queue> <count>
//       a consumer first, on a queue the other node does not have yet,
//       then messages sent there (creating it): the consumer's node
//       federates them once it hears the queue was added.
// tests/interop/cluster/amqp-federation.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class AmqpFederation {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a federation receiver takes to attach. */
    static final long PROPAGATION = 3000;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "topic" -> topic(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
            case "queue" -> queue(args[1], args[2], args[3], Integer.parseInt(args[4]), false);
            case "late" -> queue(args[1], args[2], args[3], Integer.parseInt(args[4]), true);
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    /** The hops a federated message came with: a Core property when it
     *  was tunneled, else the AMQP annotation, which Core sees as a
     *  property too. */
    static String hops(Message m) throws JMSException {
        Object h = m.getObjectProperty("_AMQ_Fed_Hops");
        if (h == null) {
            h = m.getObjectProperty("JMS_AMQP_MA_x-opt-amq-fed-hops");
        }
        return String.valueOf(h);
    }

    static void topic(String subUrl, String pubUrl, String topic, int count, String clientId) throws Exception {
        try (Connection sub = core(subUrl); Connection pub = core(pubUrl)) {
            sub.setClientID(clientId);
            Session ss = sub.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer subscriber = ss.createDurableSubscriber(ss.createTopic(topic), "sub");
            sub.start();
            Thread.sleep(PROPAGATION);
            Session ps = pub.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = ps.createProducer(ps.createTopic(topic));
            for (int i = 0; i < count; i++) {
                p.send(ps.createTextMessage("m" + i));
            }
            System.out.println("published " + count);
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
            System.out.println("received " + got);
            System.out.println("hops " + hops);
        }
    }

    static void queue(String sendUrl, String receiveUrl, String queue, int count, boolean late) throws Exception {
        try (Connection sc = core(sendUrl); Connection rc = core(receiveUrl)) {
            Session ss = sc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session rs = rc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = null;
            if (late) {
                consumer = rs.createConsumer(rs.createQueue(queue));
                rc.start();
                Thread.sleep(PROPAGATION);
            }
            MessageProducer p = ss.createProducer(ss.createQueue(queue));
            for (int i = 0; i < count; i++) {
                p.send(ss.createTextMessage("m" + i));
            }
            System.out.println("sent " + count);
            if (!late) {
                consumer = rs.createConsumer(rs.createQueue(queue));
                rc.start();
            }
            List<String> got = new ArrayList<>();
            while (got.size() < count) {
                Message m = consumer.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
            }
            System.out.println("received " + got);
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
            consumer.close();
            Thread.sleep(1000);
            sc.start();
            MessageConsumer left = ss.createConsumer(ss.createQueue(queue));
            System.out.println(left.receive(1000) == null ? "none left" : "MORE left");
        }
    }
}
