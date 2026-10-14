// The Artemis client against a primary and its backup.
//   java -cp '<artemis>/lib/*' Failover.java topology <url>
//       prints the pair of connectors the broker's topology gives, once it
//       has a backup.
//   java -cp '<artemis>/lib/*' Failover.java failover <url> <marker> <count>
//       a JMS connection with ha=true sends <count> messages to queue
//       failover, waits for <marker>.1 (the primary killed), for the
//       failover to complete, then receives them; sends <count> more,
//       waits for <marker>.2 (failing back), for the failover, and receives
//       them again.
//   java -cp '<artemis>/lib/*' Failover.java failover-once <url> <marker> <count>
//       the same, but the second round has no failover: it sends and
//       receives on the server it failed over to.
// tests/interop/ha/failback.sh and replication.sh run it; it prints what it
// saw.

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Failover {
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "topology" -> topology(args[1]);
            case "failover" -> failover(args[1], args[2], Integer.parseInt(args[3]), 2);
            case "failover-once" -> failover(args[1], args[2], Integer.parseInt(args[3]), 1);
            default -> throw new IllegalArgumentException(args[0]);
        }
        System.exit(0);
    }

    static String hostPort(TransportConfiguration tc) {
        if (tc == null) {
            return "none";
        }
        return tc.getParams().get("host") + ":" + tc.getParams().get("port");
    }

    static void topology(String url) throws Exception {
        CompletableFuture<String> pair = new CompletableFuture<>();
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url)) {
            locator.addClusterTopologyListener(new ClusterTopologyListener() {
                @Override
                public void nodeUP(TopologyMember member, boolean last) {
                    if (member.getBackup() != null) {
                        pair.complete("primary=" + hostPort(member.getPrimary()) + " backup=" + hostPort(member.getBackup()));
                    }
                }

                @Override
                public void nodeDown(long eventUID, String nodeID) {
                }
            });
            try (ClientSessionFactory sf = locator.createSessionFactory();
                 ClientSession session = sf.createSession("admin", "admin", false, true, true, false, 1)) {
                System.out.println("topology: " + pair.get(30, TimeUnit.SECONDS));
            }
        }
    }

    static void await(Path file) throws Exception {
        for (int i = 0; i < 6000 && !Files.exists(file); i++) {
            Thread.sleep(100);
        }
        if (!Files.exists(file)) {
            throw new java.lang.IllegalStateException("no " + file);
        }
    }

    static void failover(String url, String marker, int count, int failovers) throws Exception {
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url, "admin", "admin");
        try (Connection connection = cf.createConnection()) {
            ((ActiveMQConnection) connection).setFailoverListener(e -> events.add(e.name()));
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            javax.jms.Queue queue = session.createQueue("failover");
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);
            for (int round = 1; round <= 2; round++) {
                for (int i = 0; i < count; i++) {
                    producer.send(session.createTextMessage("round " + round + " message " + i));
                }
                session.commit();
                System.out.println("sent " + count);
                if (round <= failovers) {
                    await(Paths.get(marker + "." + round));
                    String event;
                    do {
                        event = events.poll(60, TimeUnit.SECONDS);
                        System.out.println("failover event: " + event);
                    } while (event != null && !event.equals("FAILOVER_COMPLETED") && !event.equals("FAILOVER_FAILED"));
                }
                int received = 0;
                while (received < count) {
                    TextMessage m = (TextMessage) consumer.receive(10000);
                    if (m == null) {
                        break;
                    }
                    if (!m.getText().equals("round " + round + " message " + received)) {
                        System.out.println("unexpected: " + m.getText());
                    }
                    received++;
                }
                session.commit();
                System.out.println("received " + received);
            }
        }
    }
}
