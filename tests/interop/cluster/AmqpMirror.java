// The clients of the AMQP mirror suite, with the Artemis client (Core):
//   java -cp '<artemis>/lib/*' AmqpMirror.java create <url> <address> <queue> anycast|multicast
//       makes the address and its queue (Core management of the session).
//   java -cp '<artemis>/lib/*' AmqpMirror.java delete <url> <queue>
//       deletes the queue.
//   java -cp '<artemis>/lib/*' AmqpMirror.java send <url> <address> <prefix> <count>
//       sends <prefix>0, <prefix>1, ... to the address (JMS, durable).
//   java -cp '<artemis>/lib/*' AmqpMirror.java count <url> <queue> <expected>
//       waits up to 20 s for the queue to hold <expected> messages (-1: for
//       it not to exist), then prints what it holds.
//   java -cp '<artemis>/lib/*' AmqpMirror.java consume <url> <queue> <count>
//       takes <count> messages and prints them, sorted.
// tests/interop/cluster/amqp-mirror.sh runs it; it prints what it saw.

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

public class AmqpMirror {
    static final String USER = "admin", PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "create" -> create(args[1], args[2], args[3], args[4]);
            case "delete" -> delete(args[1], args[2]);
            case "send" -> send(args[1], args[2], args[3], Integer.parseInt(args[4]));
            case "count" -> count(args[1], args[2], Long.parseLong(args[3]));
            case "consume" -> consume(args[1], args[2], Integer.parseInt(args[3]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    interface Work {
        void run(ClientSession session) throws Exception;
    }

    static void withSession(String url, Work work) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory factory = locator.createSessionFactory();
             ClientSession session = factory.createSession(USER, PASSWORD, false, true, true, false, 1)) {
            work.run(session);
        }
    }

    static void create(String url, String address, String queue, String routing) throws Exception {
        RoutingType type = routing.equals("anycast") ? RoutingType.ANYCAST : RoutingType.MULTICAST;
        withSession(url, s -> {
            s.createAddress(SimpleString.of(address), EnumSet.of(type), false);
            s.createQueue(QueueConfiguration.of(queue).setAddress(address).setRoutingType(type).setDurable(true));
        });
        System.out.println("created " + address + "::" + queue);
    }

    static void delete(String url, String queue) throws Exception {
        withSession(url, s -> s.deleteQueue(SimpleString.of(queue)));
        System.out.println("deleted " + queue);
    }

    static void send(String url, String address, String prefix, int count) throws Exception {
        try (Connection c = new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination d = address.startsWith("topic:") ? s.createTopic(address.substring(6)) : s.createQueue(address);
            MessageProducer p = s.createProducer(d);
            for (int i = 0; i < count; i++) {
                p.send(s.createTextMessage(prefix + i));
            }
        }
        System.out.println("sent " + count);
    }

    static void count(String url, String queue, long expected) throws Exception {
        withSession(url, s -> {
            long deadline = System.currentTimeMillis() + 20000;
            while (true) {
                ClientSession.QueueQuery q = s.queueQuery(SimpleString.of(queue));
                long n = q.isExists() ? q.getMessageCount() : -1;
                if (n == expected || System.currentTimeMillis() > deadline) {
                    System.out.println(n < 0 ? queue + ": none" : queue + ": " + n + " messages");
                    return;
                }
                Thread.sleep(100);
            }
        });
    }

    static void consume(String url, String queue, int count) throws Exception {
        try (Connection c = new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            List<String> got = new ArrayList<>();
            while (got.size() < count) {
                Message m = consumer.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
            }
            Collections.sort(got);
            System.out.println("received " + got);
        }
    }
}
