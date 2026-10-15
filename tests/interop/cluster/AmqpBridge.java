// The clients of the AMQP bridge suite, with the Artemis JMS client
// (Core):
//   java -cp '<artemis>/lib/*' AmqpBridge.java topic <sub-url> <pub-url> <topic> <count> <client-id>
//       a durable subscriber on one node, then messages published on the
//       other: the bridge moves them. Prints the distinct messages received.
//   java -cp '<artemis>/lib/*' AmqpBridge.java queue <send-url> <send-queue> <receive-url> <receive-queue> <count>
//       messages sent to a queue on one node, then a consumer of a queue on
//       the other: the bridge moves them, and the first queue is left with
//       none.
//   java -cp '<artemis>/lib/*' AmqpBridge.java late <send-url> <receive-url> <queue> <count>
//       a consumer first, on a queue the other node does not have and will
//       not create: the bridge's receiver is refused. Then the queue is
//       made there (Core management of the session) and messages sent to
//       it: the receiver is attached again (link recovery) and takes them.
// tests/interop/cluster/amqp-bridge.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class AmqpBridge {
    static final String USER = "admin", PASSWORD = "admin";
    /** How long a bridge receiver takes to attach. */
    static final long PROPAGATION = 3000;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "topic" -> topic(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
            case "queue" -> queue(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]));
            case "late" -> late(args[1], args[2], args[3], Integer.parseInt(args[4]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
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
            while (got.size() < count) {
                Message m = subscriber.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
            }
            for (Message m; (m = subscriber.receive(1000)) != null; ) {
                got.add(((TextMessage) m).getText());
            }
            System.out.println("received " + got);
        }
    }

    static List<String> receive(MessageConsumer consumer, int count) throws JMSException {
        List<String> got = new ArrayList<>();
        while (got.size() < count) {
            Message m = consumer.receive(15000);
            if (m == null) {
                break;
            }
            got.add(((TextMessage) m).getText());
        }
        return got;
    }

    static void queue(String sendUrl, String sendQueue, String receiveUrl, String receiveQueue, int count) throws Exception {
        try (Connection sc = core(sendUrl); Connection rc = core(receiveUrl)) {
            Session ss = sc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session rs = rc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = ss.createProducer(ss.createQueue(sendQueue));
            for (int i = 0; i < count; i++) {
                p.send(ss.createTextMessage("m" + i));
            }
            System.out.println("sent " + count);
            MessageConsumer consumer = rs.createConsumer(rs.createQueue(receiveQueue));
            rc.start();
            System.out.println("received " + receive(consumer, count));
            System.out.println(consumer.receive(500) == null ? "no more" : "MORE");
            consumer.close();
            Thread.sleep(1000);
            sc.start();
            MessageConsumer left = ss.createConsumer(ss.createQueue(sendQueue));
            System.out.println(left.receive(1000) == null ? "none left" : "MORE left");
        }
    }

    static void late(String sendUrl, String receiveUrl, String queue, int count) throws Exception {
        try (Connection rc = core(receiveUrl)) {
            Session rs = rc.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = rs.createConsumer(rs.createQueue(queue));
            rc.start();
            Thread.sleep(PROPAGATION);
            try (ServerLocator locator = ActiveMQClient.createServerLocator(sendUrl);
                 ClientSessionFactory factory = locator.createSessionFactory();
                 ClientSession session = factory.createSession(USER, PASSWORD, false, true, true, false, 1)) {
                session.createAddress(SimpleString.of(queue), EnumSet.of(RoutingType.ANYCAST), false);
                session.createQueue(QueueConfiguration.of(queue).setRoutingType(RoutingType.ANYCAST).setDurable(true));
            }
            System.out.println("created " + queue);
            try (Connection sc = core(sendUrl)) {
                Session ss = sc.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer p = ss.createProducer(ss.createQueue(queue));
                for (int i = 0; i < count; i++) {
                    p.send(ss.createTextMessage("m" + i));
                }
                System.out.println("sent " + count);
            }
            System.out.println("received " + receive(consumer, count));
        }
    }
}
