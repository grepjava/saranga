// The clients of the linearizability runs, with the Artemis JMS client,
// recording a history for saranga-chaos check (tools/saranga-chaos/src/history.rs).
//   java -cp '<artemis>/lib/*' History.java run <url> <queue> <marker> <history> [producers] [consumers]
//       <producers> (default 3) processes each send a message to <queue> and
//       commit, a value each, until <marker> exists; <consumers> (default 3)
//       each receive a message and commit, until the producers have stopped,
//       every value whose send was committed has been received and the
//       queue gives nothing for two seconds, or nothing has come for
//       CHAOS_IDLE seconds (default 120). Each operation is written to
//       <history> as it completes: ok when its commit returned, fail when
//       the transaction was rolled back (or the send failed before the
//       commit), info when the commit failed otherwise and the client cannot
//       tell. A process whose operation is info is replaced by a new one, as
//       in Jepsen. Times are microseconds since the start, from one clock.
//       It prints "enqueued <n>" every hundred committed sends, and
//       "failed: <exception>" if a process ends on one.
// scripts/linearizability.sh runs it.

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class History {
    static final long START = System.nanoTime();
    static PrintWriter history;

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "run" -> run(args[1], args[2], args[3], args[4],
                args.length > 5 ? Integer.parseInt(args[5]) : 3,
                args.length > 6 ? Integer.parseInt(args[6]) : 3);
            default -> throw new IllegalArgumentException(args[0]);
        }
        System.exit(0);
    }

    static long now() {
        return (System.nanoTime() - START) / 1000;
    }

    static synchronized void record(int process, String type, String f, int value, long invoke) {
        history.println(process + " " + type + " " + f + " " + value + " " + invoke + " " + now());
        history.flush();
    }

    static void rollback(Session session) {
        try {
            session.rollback();
        } catch (JMSException ignored) {
        }
    }

    static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void run(String url, String queue, String marker, String file, int producers, int consumers)
            throws Exception {
        history = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        int processes = producers + consumers;
        AtomicInteger next = new AtomicInteger();
        AtomicInteger enqueued = new AtomicInteger();
        Set<Integer> committed = ConcurrentHashMap.newKeySet();
        Set<Integer> taken = ConcurrentHashMap.newKeySet();
        AtomicLong lastTaken = new AtomicLong(System.nanoTime());
        AtomicInteger producing = new AtomicInteger(producers);
        long idleLimit = TimeUnit.SECONDS.toNanos(Long.parseLong(System.getenv().getOrDefault("CHAOS_IDLE", "120")));

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            int first = i;
            threads.add(new Thread(() -> {
                int process = first;
                ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url, "admin", "admin");
                try (Connection connection = cf.createConnection()) {
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    MessageProducer producer = session.createProducer(session.createQueue(queue));
                    while (!Files.exists(Paths.get(marker))) {
                        int value = next.getAndIncrement();
                        long invoke = now();
                        try {
                            TextMessage m = session.createTextMessage("message " + value);
                            m.setIntProperty("v", value);
                            producer.send(m);
                        } catch (JMSException e) {
                            rollback(session);
                            record(process, "fail", "enqueue", value, invoke);
                            pause();
                            continue;
                        }
                        String type;
                        try {
                            session.commit();
                            type = "ok";
                        } catch (TransactionRolledBackException e) {
                            type = "fail";
                        } catch (JMSException e) {
                            type = "info";
                            rollback(session);
                        }
                        record(process, type, "enqueue", value, invoke);
                        if (type.equals("ok")) {
                            committed.add(value);
                            if (enqueued.incrementAndGet() % 100 == 0) {
                                System.out.println("enqueued " + enqueued.get());
                            }
                        } else {
                            if (type.equals("info")) {
                                process += processes;
                            }
                            pause();
                        }
                    }
                } catch (Exception e) {
                    System.out.println("failed: " + e);
                } finally {
                    producing.decrementAndGet();
                }
            }));
        }
        for (int i = 0; i < consumers; i++) {
            int first = producers + i;
            threads.add(new Thread(() -> {
                int process = first;
                int empty = 0;
                ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url, "admin", "admin");
                try (Connection connection = cf.createConnection()) {
                    connection.start();
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
                    while (true) {
                        long invoke = now();
                        Message m;
                        try {
                            m = consumer.receive(500);
                        } catch (JMSException e) {
                            rollback(session);
                            pause();
                            continue;
                        }
                        if (m == null) {
                            empty++;
                            if (producing.get() == 0 && empty >= 4 && taken.containsAll(committed)
                                || producing.get() == 0 && System.nanoTime() - lastTaken.get() > idleLimit) {
                                break;
                            }
                            continue;
                        }
                        empty = 0;
                        int value;
                        try {
                            value = m.getIntProperty("v");
                            if (!(m instanceof TextMessage t) || !t.getText().equals("message " + value)) {
                                System.out.println("corrupt: " + value);
                            }
                        } catch (JMSException e) {
                            rollback(session);
                            continue;
                        }
                        String type;
                        try {
                            session.commit();
                            type = "ok";
                        } catch (TransactionRolledBackException e) {
                            type = "fail";
                        } catch (JMSException e) {
                            type = "info";
                            rollback(session);
                        }
                        record(process, type, "dequeue", value, invoke);
                        if (!type.equals("fail")) {
                            taken.add(value);
                            lastTaken.set(System.nanoTime());
                        }
                        if (type.equals("info")) {
                            process += processes;
                        }
                    }
                } catch (Exception e) {
                    System.out.println("failed: " + e);
                }
            }));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        history.close();
        System.out.println("done: enqueued " + enqueued.get() + ", " + taken.size() + " values dequeued");
    }
}
