// The load the chaos soak checks, with the Artemis JMS client.
//   java -cp '<artemis>/lib/*' Chaos.java load <producer-url> <consumer-urls> <queue> <marker> [large-every]
//       a transacted producer on <producer-url> sends messages to <queue>,
//       ten to a transaction, each with a duplicate ID, until <marker>
//       exists; every <large-every>-th (default 0, none) is a large message.
//       A transacted consumer on each of <consumer-urls> (comma separated)
//       receives from <queue> until every committed message is received or
//       nothing has come for CHAOS_IDLE seconds (default 120) after the
//       producer stopped. A transaction rolled back is sent or received
//       again; a send whose outcome is unknown is sent again (the duplicate
//       IDs reject it if it was committed), and a receive whose commit's
//       outcome is unknown is not counted either way. It prints "committed
//       <n>" every hundred, then what was lost: a message whose transaction
//       committed and that was never received, or one received by two
//       commits that succeeded.
// scripts/chaos-soak.sh runs it.

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Chaos {
    static final int BATCH = 10;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "load" -> load(args[1], args[2].split(","), args[3], args[4],
                args.length > 5 ? Integer.parseInt(args[5]) : 0);
            default -> throw new IllegalArgumentException(args[0]);
        }
        System.exit(0);
    }

    static boolean duplicate(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException
                || (t.getMessage() != null && t.getMessage().contains("Duplicate message detected"))) {
                return true;
            }
        }
        return e instanceof JMSException j && j.getLinkedException() != null && duplicate(j.getLinkedException());
    }

    static void load(String producerUrl, String[] consumerUrls, String queue, String marker, int largeEvery)
            throws Exception {
        Set<Integer> committed = ConcurrentHashMap.newKeySet();
        Map<Integer, Integer> received = new ConcurrentHashMap<>();
        Set<Integer> unknown = ConcurrentHashMap.newKeySet();
        AtomicInteger producerRetries = new AtomicInteger(), consumerRetries = new AtomicInteger();
        AtomicLong lastReceived = new AtomicLong(System.nanoTime());
        long idleLimit = TimeUnit.SECONDS.toNanos(Long.parseLong(System.getenv().getOrDefault("CHAOS_IDLE", "120")));
        String large = "x".repeat(150 * 1024);

        Thread producer = new Thread(() -> {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(producerUrl, "admin", "admin");
            try (Connection connection = cf.createConnection()) {
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                MessageProducer p = session.createProducer(session.createQueue(queue));
                for (int n = 0; !Files.exists(Paths.get(marker)); n += BATCH) {
                    boolean again = false;
                    while (true) {
                        try {
                            for (int i = n; i < n + BATCH; i++) {
                                boolean big = largeEvery > 0 && i % largeEvery == largeEvery - 1;
                                TextMessage m = session.createTextMessage(big ? large : "message " + i);
                                m.setIntProperty("n", i);
                                m.setStringProperty("_AMQ_DUPL_ID", "chaos-" + i);
                                p.send(m);
                            }
                            session.commit();
                            break;
                        } catch (JMSException e) {
                            if (again && duplicate(e)) {
                                break;
                            }
                            producerRetries.incrementAndGet();
                            again = true;
                            try {
                                session.rollback();
                            } catch (JMSException ignored) {
                            }
                            Thread.sleep(100);
                        }
                    }
                    for (int i = n; i < n + BATCH; i++) {
                        committed.add(i);
                    }
                    if ((n + BATCH) % 100 == 0) {
                        System.out.println("committed " + (n + BATCH));
                    }
                    Thread.sleep(10);
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        });

        List<Thread> consumers = new ArrayList<>();
        for (String url : consumerUrls) {
            consumers.add(new Thread(() -> {
                ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url, "admin", "admin");
                try (Connection connection = cf.createConnection()) {
                    connection.start();
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
                    List<Integer> pending = new ArrayList<>();
                    while (true) {
                        Message m = null;
                        boolean committing = false;
                        try {
                            m = consumer.receive(500);
                            if (m != null) {
                                pending.add(m.getIntProperty("n"));
                                if (m instanceof TextMessage t && t.getText().length() == large.length()
                                    && !t.getText().equals(large)) {
                                    System.out.println("corrupt: message " + m.getIntProperty("n"));
                                }
                            }
                            if (!pending.isEmpty() && (m == null || pending.size() >= BATCH)) {
                                committing = true;
                                session.commit();
                                for (int n : pending) {
                                    received.merge(n, 1, Integer::sum);
                                }
                                pending.clear();
                                lastReceived.set(System.nanoTime());
                            }
                        } catch (TransactionRolledBackException e) {
                            consumerRetries.incrementAndGet();
                            pending.clear();
                        } catch (JMSException e) {
                            // A commit that may have happened; a receive the
                            // failure interrupted is committed or rolled back
                            // with the rest.
                            if (committing) {
                                consumerRetries.incrementAndGet();
                                unknown.addAll(pending);
                                pending.clear();
                                try {
                                    session.rollback();
                                } catch (JMSException ignored) {
                                }
                            }
                        }
                        if (m == null && !producer.isAlive() && pending.isEmpty()
                            && (committed.stream().allMatch(n -> received.containsKey(n) || unknown.contains(n))
                                || System.nanoTime() - lastReceived.get() > idleLimit)) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                }
            }));
        }
        producer.start();
        consumers.forEach(Thread::start);
        producer.join();
        lastReceived.set(System.nanoTime());
        for (Thread consumer : consumers) {
            consumer.join();
        }

        int lost = 0, twice = 0;
        for (int n : new TreeSet<>(committed)) {
            if (!received.containsKey(n) && !unknown.contains(n)) {
                lost++;
                if (lost <= 20) {
                    System.out.println("lost: message " + n);
                }
            }
        }
        for (Map.Entry<Integer, Integer> e : new TreeMap<>(received).entrySet()) {
            if (e.getValue() > 1) {
                twice++;
                if (twice <= 20) {
                    System.out.println("received twice: message " + e.getKey());
                }
            }
        }
        System.err.println("retried: " + producerRetries + " producer, " + consumerRetries
            + " consumer transactions; outcome unknown: " + unknown.size());
        System.out.println("committed " + committed.size() + ", received " + received.size());
        System.out.println("done: lost " + lost + ", received twice " + twice);
    }
}
