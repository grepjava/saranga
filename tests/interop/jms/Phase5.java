// Phase 5 interop suite: large messages and paging, which Saranga must handle as Artemis does.
//   java -cp '<artemis>/lib/*' Phase5.java tcp://host:port [test-name...]
// The broker must run tests/interop/jms/phase5.broker.xml:
//   CONFIG=tests/interop/jms/phase5.broker.xml SUITE=Phase5 scripts/interop.sh [saranga|artemis]
// Each test uses its own queues (p5.<setting-prefix>.<test>.<run>); messages that land on
// shared queues (p5.DLA, p5.EXP, divert queues) are tagged with a per-run "p5id" property
// and picked out with a selector.
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Phase5 {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static final int MB = 1024 * 1024;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        // large messages
        tests.put("largeBytes", Phase5::largeBytes);
        tests.put("largeText", Phase5::largeText);
        tests.put("largeNonPersistent", Phase5::largeNonPersistent);
        tests.put("largeCompressed", Phase5::largeCompressed);
        tests.put("largeCompressedRandom", Phase5::largeCompressedRandom);
        tests.put("largeStreamed", Phase5::largeStreamed);
        tests.put("largeTransacted", Phase5::largeTransacted);
        tests.put("largeRedelivered", Phase5::largeRedelivered);
        tests.put("largeToDeadLetter", Phase5::largeToDeadLetter);
        tests.put("largeExpiry", Phase5::largeExpiry);
        tests.put("largeBrowse", Phase5::largeBrowse);
        tests.put("largeTopicFanout", Phase5::largeTopicFanout);
        tests.put("largeSelector", Phase5::largeSelector);
        tests.put("largeSmallWindow", Phase5::largeSmallWindow);
        tests.put("largeMany", Phase5::largeMany);
        tests.put("largeFromRegular", Phase5::largeFromRegular);
        tests.put("largeDivertCopy", Phase5::largeDivertCopy);
        tests.put("largeScheduled", Phase5::largeScheduled);
        tests.put("largePreAck", Phase5::largePreAck);
        tests.put("largeDuplicate", Phase5::largeDuplicate);
        tests.put("largeCoreStream", Phase5::largeCoreStream);
        tests.put("largeConsumerClosedMidway", Phase5::largeConsumerClosedMidway);
        // paging
        tests.put("pageAndDepage", Phase5::pageAndDepage);
        tests.put("pageMessageCount", Phase5::pageMessageCount);
        tests.put("pageNonPersistent", Phase5::pageNonPersistent);
        tests.put("pageTopic", Phase5::pageTopic);
        tests.put("pageDurableSubscriptions", Phase5::pageDurableSubscriptions);
        tests.put("pageTransacted", Phase5::pageTransacted);
        tests.put("pageConsumeRollback", Phase5::pageConsumeRollback);
        tests.put("pageSelector", Phase5::pageSelector);
        tests.put("pageToDeadLetter", Phase5::pageToDeadLetter);
        tests.put("pageLarge", Phase5::pageLarge);
        tests.put("pageExpiry", Phase5::pageExpiry);
        tests.put("pageBrowse", Phase5::pageBrowse);
        tests.put("pageMaxMessages", Phase5::pageMaxMessages);
        tests.put("pageMany", Phase5::pageMany);
        tests.put("blockPolicy", Phase5::blockPolicy);
        tests.put("failPolicy", Phase5::failPolicy);
        tests.put("dropPolicy", Phase5::dropPolicy);
        tests.put("pageLimit", Phase5::pageLimit);

        int failed = 0;
        for (Map.Entry<String, Test> t : tests.entrySet()) {
            if (!only.isEmpty() && !only.contains(t.getKey())) continue;
            long start = System.nanoTime();
            try {
                t.getValue().run();
                System.out.printf("PASS %-30s %5d ms%n", t.getKey(), (System.nanoTime() - start) / 1_000_000);
            } catch (Throwable e) {
                failed++;
                System.out.printf("FAIL %-30s %s%n", t.getKey(), e);
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ----------------------------------------------------------- large messages

    static void largeBytes() throws Exception {
        String q = name("large.bytes");
        with(c -> {
            byte[] body = pattern(2 * MB + 17, 1);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = s.createBytesMessage();
                m.writeBytes(body);
                m.setStringProperty("color", "blue");
                m.setIntProperty("n", 42);
                s.createProducer(s.createQueue(q)).send(m);
                Message got = s.createConsumer(s.createQueue(q)).receive(5000);
                checkBytes(got, body);
                eq("blue", got.getStringProperty("color"), "string property");
                eq(42, got.getIntProperty("n"), "int property");
                eq(DeliveryMode.PERSISTENT, got.getJMSDeliveryMode(), "delivery mode");
                check(got.propertyExists("_AMQ_LARGE_SIZE"), "_AMQ_LARGE_SIZE is set");
            }
            check(take(c, q, null, 300).isEmpty(), "the queue is empty");
        });
    }

    static void largeText() throws Exception {
        String q = name("large.text");
        with(c -> {
            StringBuilder sb = new StringBuilder();
            while (sb.length() < 300_000) sb.append("saranga large text ").append(sb.length()).append('\n');
            String text = sb.toString();
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(s.createTextMessage(text));
                eq(text, text(s.createConsumer(s.createQueue(q)).receive(5000)), "text");
            }
        });
    }

    static void largeNonPersistent() throws Exception {
        String q = name("large.np");
        with(c -> {
            byte[] body = pattern(MB, 2);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                p.send(bytes(s, body));
                Message got = s.createConsumer(s.createQueue(q)).receive(5000);
                checkBytes(got, body);
                eq(DeliveryMode.NON_PERSISTENT, got.getJMSDeliveryMode(), "delivery mode");
            }
        });
    }

    static void largeCompressed() throws Exception {
        String q = name("large.zip");
        // Compressible: the compressed body is far smaller than the original.
        byte[] body = new byte[3 * MB];
        for (int i = 0; i < body.length; i++) body[i] = (byte) ("abcdefgh".charAt((i / 1000) % 8));
        with("compressLargeMessage=true", c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                s.createProducer(s.createQueue(q)).send(bytes(s, body));
                checkBytes(s.createConsumer(s.createQueue(q)).receive(5000), body);
            }
        });
        // A consumer without compression reads it too: the client decompresses.
        with("compressLargeMessage=true", c -> sendBytes(c, q, body));
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                checkBytes(s.createConsumer(s.createQueue(q)).receive(5000), body);
            }
        });
    }

    static void largeCompressedRandom() throws Exception {
        String q = name("large.ziprand");
        byte[] body = new byte[MB];
        new Random(5).nextBytes(body);
        with("compressLargeMessage=true", c -> {
            sendBytes(c, q, body);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                checkBytes(s.createConsumer(s.createQueue(q)).receive(5000), body);
            }
        });
    }

    static void largeStreamed() throws Exception {
        String q = name("large.stream");
        long size = 12L * MB + 3;
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = s.createBytesMessage();
                m.setObjectProperty("JMS_AMQ_InputStream", new PatternStream(size, 3));
                s.createProducer(s.createQueue(q)).send(m);
                BytesMessage got = (BytesMessage) s.createConsumer(s.createQueue(q)).receive(10000);
                check(got != null, "no message");
                eq(size, got.getBodyLength(), "body length");
                CheckingStream out = new CheckingStream(3);
                got.setObjectProperty("JMS_AMQ_SaveStream", out);
                eq(size, out.count, "bytes streamed");
                check(out.ok, "streamed content matches");
            }
        });
    }

    static void largeTransacted() throws Exception {
        String q = name("large.tx");
        with(c -> {
            byte[] body = pattern(MB + 5, 4);
            try (Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.send(bytes(s, pattern(MB, 9)));
                s.rollback();
                p.send(bytes(s, body));
                s.commit();
            }
            List<Message> got = take(c, q, null, 5000);
            eq(1, got.size(), "messages after rollback then commit");
            checkBytes(got.get(0), body);
        });
    }

    static void largeRedelivered() throws Exception {
        String q = name("large.redeliver");
        with(c -> {
            byte[] body = pattern(MB + 1, 5);
            sendBytes(c, q, body);
            try (Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                Message first = mc.receive(5000);
                checkBytes(first, body);
                check(!first.getJMSRedelivered(), "first delivery is not redelivered");
                s.rollback();
                Message again = mc.receive(5000);
                checkBytes(again, body);
                check(again.getJMSRedelivered(), "redelivered");
                eq(2, again.getIntProperty("JMSXDeliveryCount"), "delivery count");
                s.commit();
            }
            check(take(c, q, null, 300).isEmpty(), "the queue is empty");
        });
    }

    static void largeToDeadLetter() throws Exception {
        // p5.ldla.#: max-delivery-attempts 2, dead-letter-address p5.DLA
        String q = name("ldla.q");
        String id = q + ".1";
        with(c -> {
            byte[] body = pattern(MB + 9, 6);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = bytes(s, body);
                m.setStringProperty("p5id", id);
                s.createProducer(s.createQueue(q)).send(m);
            }
            eq(2, receiveAndRollBack(c, q), "deliveries before dead-lettering");
            List<Message> dead = take(c, "p5.DLA", byId(id), 3000);
            eq(1, dead.size(), "dead-lettered copies");
            checkBytes(dead.get(0), body);
            eq(q, dead.get(0).getStringProperty("_AMQ_ORIG_QUEUE"), "original queue");
            check(take(c, q, null, 300).isEmpty(), "the queue is empty");
        });
    }

    static void largeExpiry() throws Exception {
        // p5.lexp.#: expiry-address p5.EXP, expiry scan every 500 ms
        String q = name("lexp.q");
        String id = q + ".1";
        with(c -> {
            byte[] body = pattern(MB, 7);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = bytes(s, body);
                m.setStringProperty("p5id", id);
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setTimeToLive(300);
                p.send(m);
            }
            Thread.sleep(1500);
            List<Message> expired = take(c, "p5.EXP", byId(id), 3000);
            eq(1, expired.size(), "expired copies");
            checkBytes(expired.get(0), body);
            check(expired.get(0).propertyExists("_AMQ_ACTUAL_EXPIRY"), "_AMQ_ACTUAL_EXPIRY is set");
            check(take(c, q, null, 300).isEmpty(), "the queue is empty");
        });
    }

    static void largeBrowse() throws Exception {
        String q = name("large.browse");
        with(c -> {
            byte[] a = pattern(MB, 10), b = pattern(MB / 2 + 100_000, 11);
            sendBytes(c, q, a);
            sendBytes(c, q, b);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Enumeration<?> e = s.createBrowser(s.createQueue(q)).getEnumeration();
                List<Message> seen = new ArrayList<>();
                while (e.hasMoreElements()) seen.add(keepBody((Message) e.nextElement()));
                eq(2, seen.size(), "browsed messages");
                checkBytes(seen.get(0), a);
                checkBytes(seen.get(1), b);
            }
            List<Message> got = take(c, q, null, 5000);
            eq(2, got.size(), "messages still there after browsing");
            checkBytes(got.get(0), a);
            checkBytes(got.get(1), b);
        });
    }

    static void largeTopicFanout() throws Exception {
        String t = name("large.topic");
        with(c -> {
            byte[] body = pattern(2 * MB, 12);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = s.createTopic(t);
                MessageConsumer one = s.createConsumer(topic);
                MessageConsumer two = s.createConsumer(topic);
                s.createProducer(topic).send(bytes(s, body));
                checkBytes(one.receive(5000), body);
                checkBytes(two.receive(5000), body);
            }
        });
    }

    static void largeSelector() throws Exception {
        String q = name("large.selector");
        with(c -> {
            byte[] red = pattern(MB, 13), blue = pattern(MB, 14);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                BytesMessage m = bytes(s, red);
                m.setStringProperty("color", "red");
                p.send(m);
                m = bytes(s, blue);
                m.setStringProperty("color", "blue");
                p.send(m);
            }
            List<Message> got = take(c, q, "color = 'blue'", 5000);
            eq(1, got.size(), "blue messages");
            checkBytes(got.get(0), blue);
            got = take(c, q, null, 5000);
            eq(1, got.size(), "red messages left");
            checkBytes(got.get(0), red);
        });
    }

    static void largeSmallWindow() throws Exception {
        String q = name("large.window");
        for (String window : new String[] {"consumerWindowSize=1024", "consumerWindowSize=0", "consumerWindowSize=-1"}) {
            with(window, c -> {
                byte[] a = pattern(3 * MB, 15), b = pattern(MB + 3, 16);
                sendBytes(c, q, a);
                sendBytes(c, q, b);
                try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageConsumer mc = s.createConsumer(s.createQueue(q));
                    checkBytes(mc.receive(5000), a);
                    checkBytes(mc.receive(5000), b);
                    check(mc.receive(300) == null, "nothing more with " + window);
                }
            });
        }
    }

    static void largeMany() throws Exception {
        String q = name("large.many");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 25; i++) {
                    BytesMessage m = bytes(s, pattern(200_000 + i, 100 + i));
                    m.setIntProperty("i", i);
                    p.send(m);
                }
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                for (int i = 0; i < 25; i++) {
                    Message m = mc.receive(5000);
                    checkBytes(m, pattern(200_000 + i, 100 + i));
                    eq(i, m.getIntProperty("i"), "order");
                }
                check(mc.receive(300) == null, "nothing more");
            }
        });
    }

    static void largeFromRegular() throws Exception {
        // A 110 KiB message sent as a regular message is bigger than the journal buffer
        // (100 KiB in phase5.broker.xml): the broker stores and delivers it as a large
        // message.
        String q = name("large.regular");
        byte[] body = pattern(110 * 1024, 17);
        with("minLargeMessageSize=" + (10 * MB), c -> sendBytes(c, q, body));
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Message got = s.createConsumer(s.createQueue(q)).receive(5000);
                checkBytes(got, body);
                eq((long) body.length, got.getLongProperty("_AMQ_LARGE_SIZE"), "_AMQ_LARGE_SIZE");
            }
        });
    }

    static void largeDivertCopy() throws Exception {
        // p5.divert.src has a non-exclusive divert to p5.divert.dst
        String id = name("large.divert");
        with(c -> {
            byte[] body = pattern(MB + 77, 18);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = bytes(s, body);
                m.setStringProperty("p5id", id);
                s.createProducer(s.createQueue("p5.divert.src")).send(m);
            }
            List<Message> src = take(c, "p5.divert.src", byId(id), 5000);
            List<Message> dst = take(c, "p5.divert.dst", byId(id), 5000);
            eq(1, src.size(), "original");
            eq(1, dst.size(), "diverted copy");
            checkBytes(src.get(0), body);
            checkBytes(dst.get(0), body);
        });
    }

    static void largeScheduled() throws Exception {
        String q = name("large.scheduled");
        with(c -> {
            byte[] body = pattern(MB, 19);
            long sent = now();
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                BytesMessage m = bytes(s, body);
                m.setLongProperty("_AMQ_SCHED_DELIVERY", sent + 1500);
                s.createProducer(s.createQueue(q)).send(m);
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                check(mc.receive(700) == null, "not delivered before its time");
                Message got = mc.receive(5000);
                checkBytes(got, body);
                check(now() - sent >= 1000, "delivered at its time");
            }
        });
    }

    static void largePreAck() throws Exception {
        String q = name("large.preack");
        with(c -> {
            byte[] body = pattern(MB + 11, 20);
            sendBytes(c, q, body);
            try (Session s = c.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE)) {
                checkBytes(s.createConsumer(s.createQueue(q)).receive(5000), body);
            }
            check(take(c, q, null, 300).isEmpty(), "pre-acknowledged");
        });
    }

    static void largeDuplicate() throws Exception {
        String q = name("large.dup");
        with(c -> {
            byte[] body = pattern(MB, 21);
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 2; i++) {
                    BytesMessage m = bytes(s, body);
                    m.setStringProperty("_AMQ_DUPL_ID", q);
                    p.send(m);
                }
            }
            List<Message> got = take(c, q, null, 5000);
            eq(1, got.size(), "duplicates dropped");
            checkBytes(got.get(0), body);
        });
    }

    static void largeCoreStream() throws Exception {
        // The Core API: setBodyInputStream to send, setOutputStream to receive while it streams.
        String q = name("large.core");
        long size = 5L * MB + 1;
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url).setMinLargeMessageSize(64 * 1024);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            s.createQueue(QueueConfiguration.of(q).setRoutingType(RoutingType.ANYCAST).setDurable(true));
            ClientProducer p = s.createProducer(q);
            ClientMessage m = s.createMessage(true);
            m.setBodyInputStream(new PatternStream(size, 22));
            p.send(m);
            s.start();
            ClientConsumer cc = s.createConsumer(q);
            ClientMessage got = cc.receive(5000);
            check(got != null, "no message");
            check(got.isLargeMessage(), "delivered as a large message");
            eq(size, got.getBodySize() + 0L, "body size");
            CheckingStream out = new CheckingStream(22);
            got.setOutputStream(out);
            check(got.waitOutputStreamCompletion(10000), "streamed in time");
            eq(size, out.count, "bytes streamed");
            check(out.ok, "streamed content matches");
            got.acknowledge();
            s.commit();
        }
    }

    static void largeConsumerClosedMidway() throws Exception {
        // A consumer that goes away part-way through a large message: it is delivered whole
        // to the next one.
        String q = name("large.midway");
        with(c -> {
            byte[] body = pattern(8 * MB, 23);
            sendBytes(c, q, body);
        });
        with("consumerWindowSize=1024", c -> {
            Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer mc = s.createConsumer(s.createQueue(q));
            // Starts receiving, then closes without reading the body.
            mc.setMessageListener(m -> { });
            Thread.sleep(50);
            s.close();
        });
        with(c -> {
            List<Message> got = take(c, q, null, 5000);
            eq(1, got.size(), "the message is still there");
            checkBytes(got.get(0), pattern(8 * MB, 23));
        });
    }

    // ------------------------------------------------------------------ paging
    // p5.page.#: max-size-bytes 200K in 64K pages; 2 KB messages page after ~80.

    static final String PAD = "p".repeat(2000);

    /** Sends {@code n} text messages "{@code prefix}-i-PAD" with an int property i. */
    static void sendN(Session s, Destination d, String prefix, int n, int mode) throws JMSException {
        MessageProducer p = s.createProducer(d);
        p.setDeliveryMode(mode);
        for (int i = 0; i < n; i++) {
            TextMessage m = s.createTextMessage(prefix + "-" + i + "-" + PAD);
            m.setIntProperty("i", i);
            p.send(m);
        }
        p.close();
    }

    /** Receives {@code n} messages "{@code prefix}-i" in order, i from {@code from}. */
    static void expectN(MessageConsumer c, String prefix, int from, int n) throws JMSException {
        for (int i = from; i < from + n; i++) {
            Message m = c.receive(5000);
            check(m != null, prefix + ": message " + i + " missing");
            eq(prefix + "-" + i + "-" + PAD, text(m), prefix + ": message " + i);
        }
    }

    static long messageCount(String queue) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession()) {
            return s.queueQuery(SimpleString.of(queue)).getMessageCount();
        }
    }

    static void pageAndDepage() throws Exception {
        String q = name("page.depage");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "m", 600, DeliveryMode.PERSISTENT);
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                expectN(mc, "m", 0, 600);
                check(mc.receive(300) == null, "nothing more");
            }
        });
    }

    static void pageMessageCount() throws Exception {
        String q = name("page.count");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "m", 500, DeliveryMode.PERSISTENT);
                eq(500L, messageCount(q), "message count with paged messages");
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                expectN(mc, "m", 0, 100);
                mc.close();
                long deadline = now() + 3000;
                while (messageCount(q) != 400 && now() < deadline) Thread.sleep(50);
                eq(400L, messageCount(q), "message count after consuming 100");
                expectN(s.createConsumer(s.createQueue(q)), "m", 100, 400);
            }
        });
    }

    static void pageNonPersistent() throws Exception {
        String q = name("page.np");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "np", 400, DeliveryMode.NON_PERSISTENT);
                expectN(s.createConsumer(s.createQueue(q)), "np", 0, 400);
            }
        });
    }

    static void pageTopic() throws Exception {
        String t = name("page.topic");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = s.createTopic(t);
                MessageConsumer one = s.createConsumer(topic);
                MessageConsumer two = s.createConsumer(topic);
                sendN(s, topic, "t", 400, DeliveryMode.PERSISTENT);
                expectN(one, "t", 0, 400);
                expectN(two, "t", 0, 400);
            }
        });
    }

    static void pageDurableSubscriptions() throws Exception {
        String t = name("page.durable");
        try (Connection c = cf(null).createConnection()) {
            c.setClientID("p5-durable-" + RUN);
            c.start();
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = s.createTopic(t);
                s.createDurableSubscriber(topic, "a").close();
                s.createDurableSubscriber(topic, "b").close();
                sendN(s, topic, "d", 400, DeliveryMode.PERSISTENT);
                try (TopicSubscriber a = s.createDurableSubscriber(topic, "a")) {
                    expectN(a, "d", 0, 400);
                }
                try (TopicSubscriber b = s.createDurableSubscriber(topic, "b")) {
                    expectN(b, "d", 0, 150);
                }
                try (TopicSubscriber b = s.createDurableSubscriber(topic, "b")) {
                    expectN(b, "d", 150, 250);
                    check(b.receive(300) == null, "nothing more for b");
                }
                s.unsubscribe("a");
                s.unsubscribe("b");
            }
        }
    }

    static void pageTransacted() throws Exception {
        String q = name("page.tx");
        with(c -> {
            try (Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
                 Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Queue queue = tx.createQueue(q);
                sendN(tx, queue, "a", 200, DeliveryMode.PERSISTENT);
                tx.commit();
                sendN(tx, queue, "b", 100, DeliveryMode.PERSISTENT);
                tx.rollback();
                sendN(tx, queue, "c", 50, DeliveryMode.PERSISTENT);
                // Not yet committed: a consumer reaches them only after the commit.
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                expectN(mc, "a", 0, 200);
                check(mc.receive(500) == null, "uncommitted paged messages are not delivered");
                tx.commit();
                expectN(mc, "c", 0, 50);
                check(mc.receive(300) == null, "rolled-back messages never arrive");
            }
        });
    }

    static void pageConsumeRollback() throws Exception {
        String q = name("page.rollback");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "r", 300, DeliveryMode.PERSISTENT);
            }
            try (Session tx = c.createSession(true, Session.SESSION_TRANSACTED)) {
                MessageConsumer mc = tx.createConsumer(tx.createQueue(q));
                expectN(mc, "r", 0, 10);
                tx.rollback();
                Message m = mc.receive(5000);
                eq("r-0-" + PAD, text(m), "redelivered first");
                check(m.getJMSRedelivered(), "redelivered");
                expectN(mc, "r", 1, 299);
                tx.commit();
            }
            check(take(c, q, null, 300).isEmpty(), "the queue is empty");
        });
    }

    static void pageSelector() throws Exception {
        String q = name("page.selector");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 400; i++) {
                    TextMessage m = s.createTextMessage("s-" + i + "-" + PAD);
                    m.setStringProperty("color", i % 2 == 0 ? "red" : "blue");
                    m.setIntProperty("i", i);
                    p.send(m);
                }
                MessageConsumer red = s.createConsumer(s.createQueue(q), "color = 'red'");
                for (int i = 0; i < 400; i += 2) {
                    eq(i, red.receive(5000).getIntProperty("i"), "red in order");
                }
                check(red.receive(300) == null, "no more red");
                red.close();
                MessageConsumer blue = s.createConsumer(s.createQueue(q));
                for (int i = 1; i < 400; i += 2) {
                    eq(i, blue.receive(5000).getIntProperty("i"), "blue in order");
                }
            }
        });
    }

    static void pageToDeadLetter() throws Exception {
        // p5.pagedla.#: paging, max-delivery-attempts 2, dead-letter-address p5.DLA
        String q = name("pagedla.q");
        String id = q + ".dead";
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 300; i++) {
                    TextMessage m = s.createTextMessage("x-" + i + "-" + PAD);
                    m.setIntProperty("i", i);
                    m.setStringProperty("p5id", i == 250 ? id : "keep");
                    p.send(m);
                }
            }
            // The 251st message is in a page: it is rolled back until dead.
            try (Session tx = c.createSession(true, Session.SESSION_TRANSACTED)) {
                MessageConsumer mc = tx.createConsumer(tx.createQueue(q), "p5id = '" + id + "'");
                for (int n = 0; n < 2; n++) {
                    check(mc.receive(5000) != null, "delivery " + n);
                    tx.rollback();
                }
                check(mc.receive(500) == null, "dead-lettered after two deliveries");
            }
            List<Message> dead = take(c, "p5.DLA", byId(id), 3000);
            eq(1, dead.size(), "dead-lettered copies");
            eq(250, dead.get(0).getIntProperty("i"), "the right message");
            eq(299, take(c, q, null, 5000).size(), "the others remain");
        });
    }

    static void pageLarge() throws Exception {
        String q = name("page.large");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "f", 150, DeliveryMode.PERSISTENT);
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 8; i++) {
                    BytesMessage m = bytes(s, pattern(300_000 + i, 40 + i));
                    m.setIntProperty("n", i);
                    p.send(m);
                }
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                expectN(mc, "f", 0, 150);
                for (int i = 0; i < 8; i++) {
                    Message m = mc.receive(5000);
                    checkBytes(m, pattern(300_000 + i, 40 + i));
                    eq(i, m.getIntProperty("n"), "large in order");
                }
            }
        });
    }

    static void pageExpiry() throws Exception {
        String q = name("page.expiry");
        String id = q + ".exp";
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                p.setTimeToLive(500);
                for (int i = 0; i < 300; i++) {
                    TextMessage m = s.createTextMessage("e-" + i + "-" + PAD);
                    m.setStringProperty("p5id", id);
                    p.send(m);
                }
            }
            Thread.sleep(1500);
            eq(0, take(c, q, null, 1500).size(), "expired paged messages are not delivered");
            long deadline = now() + 5000;
            int expired = 0;
            while (expired < 300 && now() < deadline) {
                expired += take(c, "ExpiryQueue", byId(id), 500).size();
            }
            eq(300, expired, "expired to the expiry address");
        });
    }

    static void pageBrowse() throws Exception {
        String q = name("page.browse");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "b", 300, DeliveryMode.PERSISTENT);
                Enumeration<?> e = s.createBrowser(s.createQueue(q)).getEnumeration();
                int n = 0;
                while (e.hasMoreElements()) {
                    eq("b-" + n + "-" + PAD, text((Message) e.nextElement()), "browsed in order");
                    n++;
                }
                eq(300, n, "browsed messages");
                expectN(s.createConsumer(s.createQueue(q)), "b", 0, 300);
            }
        });
    }

    static void pageMaxMessages() throws Exception {
        // p5.pagemsgs.#: max-size-messages 20
        String q = name("pagemsgs.q");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "mm", 200, DeliveryMode.PERSISTENT);
                eq(200L, messageCount(q), "message count");
                expectN(s.createConsumer(s.createQueue(q)), "mm", 0, 200);
            }
        });
    }

    static void pageMany() throws Exception {
        String q = name("page.many");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "many", 5000, DeliveryMode.PERSISTENT);
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                for (int i = 0; i < 5000; i++) {
                    Message m = mc.receive(5000);
                    check(m != null, "message " + i);
                    eq(i, m.getIntProperty("i"), "in order");
                }
                check(mc.receive(300) == null, "nothing more");
            }
        });
    }

    static void blockPolicy() throws Exception {
        // p5.block.#: max-size-bytes 200K, BLOCK
        String q = name("block.q");
        with(c -> {
            AtomicInteger sent = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageProducer p = s.createProducer(s.createQueue(q));
                    for (int i = 0; i < 300; i++) {
                        TextMessage m = s.createTextMessage("k-" + i + "-" + PAD);
                        m.setIntProperty("i", i);
                        p.send(m);
                        sent.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            producer.start();
            Thread.sleep(1500);
            check(producer.isAlive(), "the producer is blocked");
            check(sent.get() < 300, "not everything was sent: " + sent.get());
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer mc = s.createConsumer(s.createQueue(q));
                for (int i = 0; i < 300; i++) {
                    Message m = mc.receive(10000);
                    check(m != null, "message " + i);
                    eq(i, m.getIntProperty("i"), "in order");
                }
            }
            producer.join(10000);
            check(failure.get() == null, "producer failed: " + failure.get());
            eq(300, sent.get(), "all sent once unblocked");
        });
    }

    static void failPolicy() throws Exception {
        // p5.fail.#: max-size-bytes 200K, FAIL
        String q = name("fail.q");
        with(c -> {
            int sent = 0;
            JMSException failed = null;
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 300 && failed == null; i++) {
                    try {
                        p.send(s.createTextMessage("f-" + i + "-" + PAD));
                        sent++;
                    } catch (JMSException e) {
                        failed = e;
                    }
                }
            }
            check(failed != null, "a send failed");
            check(sent > 10 && sent < 300, "sends before the address was full: " + sent);
            eq(sent, take(c, q, null, 5000).size(), "every accepted message");
            // With room again, sends succeed.
            sendBytes(c, q, pattern(100, 1));
            eq(1, take(c, q, null, 5000).size(), "sent after draining");
        });
    }

    static void dropPolicy() throws Exception {
        // p5.drop.#: max-size-bytes 200K, DROP
        String q = name("drop.q");
        with(c -> {
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                sendN(s, s.createQueue(q), "x", 300, DeliveryMode.PERSISTENT);
            }
            int kept = take(c, q, null, 5000).size();
            check(kept > 10 && kept < 300, "messages kept: " + kept);
            sendBytes(c, q, pattern(100, 1));
            eq(1, take(c, q, null, 5000).size(), "sent after draining");
        });
    }

    static void pageLimit() throws Exception {
        // p5.pagelimit.#: paging, page-limit-messages 100, page-full-policy FAIL
        String q = name("pagelimit.q");
        with(c -> {
            int sent = 0;
            JMSException failed = null;
            try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageProducer p = s.createProducer(s.createQueue(q));
                for (int i = 0; i < 400 && failed == null; i++) {
                    try {
                        p.send(s.createTextMessage("l-" + i + "-" + PAD));
                        sent++;
                    } catch (JMSException e) {
                        failed = e;
                    }
                }
            }
            check(failed != null, "a send failed past the page limit");
            check(sent > 100 && sent < 400, "sends before the page limit: " + sent);
            eq(sent, take(c, q, null, 5000).size(), "every accepted message");
            sendBytes(c, q, pattern(100, 1));
            eq(1, take(c, q, null, 5000).size(), "sent after draining");
        });
    }

    // ----------------------------------------------------------------- helpers

    /** {@code p5.<base>.<run>}: the base starts with the address-settings prefix the test relies on. */
    static String name(String base) {
        return "p5." + base + "." + RUN;
    }

    static ConnectionFactory cf(String params) {
        return new ActiveMQConnectionFactory(params == null ? url : url + "?" + params);
    }

    interface ThrowingConsumer<T> { void accept(T t) throws Exception; }

    static void with(String params, ThrowingConsumer<Connection> body) throws Exception {
        try (Connection c = cf(params).createConnection()) {
            c.start();
            body.accept(c);
        }
    }

    static void with(ThrowingConsumer<Connection> body) throws Exception {
        with(null, body);
    }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static <T> void eq(T expected, T actual, String what) {
        if (!Objects.equals(expected, actual)) throw new AssertionError(what + ": expected " + expected + ", got " + actual);
    }

    static String text(Message m) throws JMSException {
        check(m != null, "no message");
        return ((TextMessage) m).getText();
    }

    /** {@code length} bytes of a pattern that differs by seed and position. */
    static byte[] pattern(int length, int seed) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) b[i] = patternByte(i, seed);
        return b;
    }

    static byte patternByte(long i, int seed) {
        return (byte) (i * 31 + seed * 7 + (i >> 13));
    }

    static BytesMessage bytes(Session s, byte[] body) throws JMSException {
        BytesMessage m = s.createBytesMessage();
        m.writeBytes(body);
        return m;
    }

    static void sendBytes(Connection c, String queue, byte[] body) throws Exception {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            s.createProducer(s.createQueue(queue)).send(bytes(s, body));
        }
    }

    /** Bodies read while their session was open: a large body cannot be read after. */
    static final Map<Message, byte[]> BODIES = Collections.synchronizedMap(new IdentityHashMap<>());

    /** Reads a BytesMessage's whole body now. */
    static Message keepBody(Message m) throws JMSException {
        if (m instanceof BytesMessage) {
            BytesMessage b = (BytesMessage) m;
            byte[] body = new byte[(int) b.getBodyLength()];
            b.readBytes(body);
            BODIES.put(m, body);
        }
        return m;
    }

    static void checkBytes(Message m, byte[] expected) throws JMSException {
        check(m != null, "no message");
        check(m instanceof BytesMessage, "a BytesMessage, got " + m.getClass().getSimpleName());
        byte[] got = BODIES.get(m);
        if (got == null) {
            BytesMessage b = (BytesMessage) m;
            eq((long) expected.length, b.getBodyLength(), "body length");
            got = new byte[expected.length];
            eq(expected.length, b.readBytes(got), "bytes read");
        }
        eq(expected.length, got.length, "body length");
        check(Arrays.equals(expected, got), "body content");
    }

    /** All messages currently on {@code queue} matching {@code selector}, consumed. */
    static List<Message> take(Connection c, String queue, String selector, long firstWait) throws Exception {
        List<Message> got = new ArrayList<>();
        try (Session s = c.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(queue), selector);
            Message m = mc.receive(firstWait);
            while (m != null) {
                got.add(keepBody(m));
                m.acknowledge();
                m = mc.receive(300);
            }
        }
        return got;
    }

    static String byId(String id) {
        return "p5id = '" + id + "'";
    }

    /** Receives on a transacted session and rolls back until the message stops coming; returns the deliveries. */
    static int receiveAndRollBack(Connection c, String queue) throws Exception {
        int n = 0;
        try (Session s = c.createSession(true, Session.SESSION_TRANSACTED)) {
            MessageConsumer mc = s.createConsumer(s.createQueue(queue));
            while (mc.receive(1500) != null) {
                n++;
                s.rollback();
                check(n < 50, "message never left the queue");
            }
        }
        return n;
    }

    static long now() {
        return System.currentTimeMillis();
    }

    /** The pattern, generated as it is read. */
    static final class PatternStream extends InputStream {
        final long size;
        final int seed;
        long at;

        PatternStream(long size, int seed) {
            this.size = size;
            this.seed = seed;
        }

        @Override
        public int read() {
            return at < size ? patternByte(at++, seed) & 0xff : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (at >= size) return -1;
            int n = (int) Math.min(len, size - at);
            for (int i = 0; i < n; i++) b[off + i] = patternByte(at++, seed);
            return n;
        }
    }

    /** Checks the pattern as it is written. */
    static final class CheckingStream extends OutputStream {
        final int seed;
        long count;
        boolean ok = true;

        CheckingStream(int seed) {
            this.seed = seed;
        }

        @Override
        public void write(int b) {
            if ((byte) b != patternByte(count, seed)) ok = false;
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) {
            for (int i = 0; i < len; i++) write(b[off + i]);
        }
    }
}
