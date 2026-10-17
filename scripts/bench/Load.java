// The client scripts/bench.sh runs for the protocols Artemis's perf client
// does not speak: STOMP 1.2, MQTT 3.1.1 (QoS 0 or 1) and OpenWire (the
// ActiveMQ 5 JMS client in Artemis's lib). One producer and one consumer on
// one queue, 1 KiB bodies, for SECONDS of which the first WARMUP are not
// measured, at RATE messages a second (0: unbounded). At most -Dinflight
// (default 1000) messages are sent and not yet received, so an unbounded
// run measures the rate a broker keeps up with rather than a backlog.
// -Dpersistent=true sends STOMP and OpenWire messages persistent; MQTT's are
// as their QoS makes them. It prints the mean sent and received a second
// after the warmup and the transfer latency (send to receipt) p50 and p99,
// then a line for bench.sh: RESULT, success or fail, the mean received a
// second, p50 and p99 in microseconds, and the messages received from three
// seconds after the warmup to the end (the window bench.sh reads the
// broker's CPU in).
//   java -cp '<artemis>/lib/*' Load.java stomp|mqtt0|mqtt1|openwire host port seconds warmup rate
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Load {
    static final int SIZE = 1024;
    static final AtomicLong sent = new AtomicLong(), received = new AtomicLong();
    static volatile boolean stop;
    static final long[] lat = new long[1 << 22];
    static final AtomicInteger nlat = new AtomicInteger();
    static volatile boolean measuring;
    // End to end: at most 1000 sent and not yet received.
    static final Semaphore inflight = new Semaphore(Integer.getInteger("inflight", 1000));
    static final boolean PERSISTENT = Boolean.getBoolean("persistent");
    static String host;
    static int port;

    public static void main(String[] a) throws Exception {
        String proto = a[0];
        host = a[1];
        port = Integer.parseInt(a[2]);
        int seconds = Integer.parseInt(a[3]), warmup = Integer.parseInt(a[4]);
        long rate = Long.parseLong(a[5]);
        Runnable consumer, producer;
        switch (proto) {
            case "stomp" -> { consumer = Load::stompConsumer; producer = () -> stompProducer(rate); }
            case "mqtt0", "mqtt1" -> {
                int qos = proto.charAt(4) - '0';
                consumer = () -> mqttConsumer(qos);
                producer = () -> mqttProducer(qos, rate);
            }
            case "openwire" -> { consumer = Load::owConsumer; producer = () -> owProducer(rate); }
            default -> throw new IllegalArgumentException(proto);
        }
        Thread c = new Thread(wrap(consumer)), p = new Thread(wrap(producer));
        c.start();
        Thread.sleep(500);
        p.start();
        long s0 = 0, r0 = 0, sum_s = 0, sum_r = 0, window0 = 0;
        int n = 0;
        for (int i = 1; i <= seconds; i++) {
            Thread.sleep(1000);
            long s = sent.get(), r = received.get();
            if (i == warmup) measuring = true;
            if (i == warmup + 3) window0 = r;
            if (i > warmup) { sum_s += s - s0; sum_r += r - r0; n++; }
            s0 = s; r0 = r;
        }
        stop = true;
        int k = Math.min(nlat.get(), lat.length);
        long[] l = Arrays.copyOf(lat, k);
        Arrays.sort(l);
        long recv = n > 0 ? sum_r / n : 0;
        long p50 = k > 0 ? l[k / 2] / 1000 : 0, p99 = k > 0 ? l[(int) (k * 0.99)] / 1000 : 0;
        System.err.printf("totals: sent %d received %d%n", sent.get(), received.get());
        System.out.printf("%s: sent %d msg/s, received %d msg/s, p50 %d us, p99 %d us%n", proto,
            n > 0 ? sum_s / n : 0, recv, p50, p99);
        System.out.printf("RESULT %s %d %d %d %d%n", recv > 0 ? "success" : "fail", recv, p50, p99,
            received.get() - window0);
        System.exit(0);
    }

    static Runnable wrap(Runnable r) {
        return () -> {
            try { r.run(); } catch (Throwable t) { if (!stop) { t.printStackTrace(); System.exit(2); } }
        };
    }

    // The body: 8 bytes of send time (nanoTime), then filler.
    static byte[] body() {
        byte[] b = new byte[SIZE];
        Arrays.fill(b, (byte) 'x');
        return b;
    }

    static void stamp(byte[] b, int off) {
        long t = System.nanoTime();
        for (int i = 0; i < 8; i++) b[off + i] = (byte) (t >>> (56 - 8 * i));
    }

    static void record(byte[] b, int off) {
        long t = 0;
        for (int i = 0; i < 8; i++) t = (t << 8) | (b[off + i] & 0xff);
        received.incrementAndGet();
        inflight.release();
        if (measuring) {
            int i = nlat.getAndIncrement();
            if (i < lat.length) lat[i] = System.nanoTime() - t;
        }
    }

    // Paces to RATE: sleeps when ahead of schedule. More than 10 ms behind
    // it (a stall), it starts over from now rather than catching up in a
    // burst, which would measure the burst's queueing as latency.
    static final class Pacer {
        final long rate;
        long start = System.nanoTime(), count;
        Flushable out;
        Pacer(long rate) { this.rate = rate; }
        Pacer(long rate, Flushable out) { this.rate = rate; this.out = out; }
        void next() {
            try {
                if (!inflight.tryAcquire()) {
                    if (out != null) out.flush();
                    inflight.acquire();
                }
            } catch (Exception e) { throw new RuntimeException(e); }
            count++;
            if (rate <= 0) return;
            long due = start + count * 1_000_000_000L / rate;
            long now = System.nanoTime();
            if (due > now) LockSupport.parkNanos(due - now);
            else if (now - due > 10_000_000L) { start = now; count = 0; }
        }
    }

    static final class LockSupport {
        static void parkNanos(long n) { java.util.concurrent.locks.LockSupport.parkNanos(n); }
    }

    static Socket socket() throws IOException {
        Socket s = new Socket(host, port);
        s.setTcpNoDelay(true);
        return s;
    }

    // A consumer only receives, and a broker closes a connection it has had
    // nothing from within its TTL (Artemis's 60 s for STOMP without
    // heart-beats, 1.5 times the MQTT keep-alive), so it pings as a real
    // client does.
    static void keepAlive(OutputStream out, byte[] ping, long ms) {
        Thread t = new Thread(wrap(() -> {
            try {
                while (!stop) {
                    Thread.sleep(ms);
                    synchronized (out) { out.write(ping); out.flush(); }
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
            catch (InterruptedException e) { }
        }));
        t.setDaemon(true);
        t.start();
    }

    // ---- STOMP 1.2: receipts every 100 sends, at most 10 outstanding.
    static void stompConnect(OutputStream out, InputStream in) throws IOException {
        out.write("CONNECT\naccept-version:1.2\nhost:localhost\nlogin:admin\npasscode:admin\n\n\0".getBytes(StandardCharsets.UTF_8));
        out.flush();
        Frame f = Frame.read(in);
        if (!f.command.equals("CONNECTED")) throw new IOException("STOMP: " + f.command + " " + f.headers);
    }

    static void stompConsumer() {
        try (Socket s = socket()) {
            InputStream in = new BufferedInputStream(s.getInputStream(), 65536);
            OutputStream out = new BufferedOutputStream(s.getOutputStream(), 65536);
            stompConnect(out, in);
            out.write("SUBSCRIBE\nid:1\ndestination:BENCH.STOMP\nsubscription-type:ANYCAST\nack:auto\n\n\0".getBytes(StandardCharsets.UTF_8));
            out.flush();
            keepAlive(out, new byte[] {'\n'}, 10_000);
            while (!stop) {
                Frame f = Frame.read(in);
                if (f.command.equals("MESSAGE")) record(f.body, 0);
                else if (f.command.equals("ERROR")) throw new IOException("STOMP ERROR " + f.headers);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static void stompProducer(long rate) {
        try (Socket s = socket()) {
            InputStream in = new BufferedInputStream(s.getInputStream(), 65536);
            OutputStream out = new BufferedOutputStream(s.getOutputStream(), 65536);
            stompConnect(out, in);
            Semaphore window = new Semaphore(Integer.getInteger("inflight", 1000) / 100);
            Thread receipts = new Thread(wrap(() -> {
                try {
                    while (!stop) {
                        Frame f = Frame.read(in);
                        if (f.command.equals("RECEIPT")) window.release();
                        else if (f.command.equals("ERROR")) throw new IOException("STOMP ERROR " + f.headers);
                    }
                } catch (IOException e) { throw new UncheckedIOException(e); }
            }));
            receipts.setDaemon(true);
            receipts.start();
            byte[] b = body();
            byte[] head = ("SEND\ndestination:BENCH.STOMP\ndestination-type:ANYCAST\n"
                + (PERSISTENT ? "persistent:true\n" : "")
                + "content-length:" + SIZE + "\n").getBytes(StandardCharsets.UTF_8);
            Pacer pacer = new Pacer(rate, out);
            for (long i = 1; !stop; i++) {
                boolean receipt = i % 100 == 0;
                if (receipt) window.acquire();
                out.write(head);
                if (receipt) out.write(("receipt:" + i + "\n").getBytes(StandardCharsets.UTF_8));
                out.write('\n');
                stamp(b, 0);
                out.write(b);
                out.write(0);
                if (rate > 0 || receipt) out.flush();
                sent.incrementAndGet();
                pacer.next();
            }
        } catch (IOException | InterruptedException e) { throw new RuntimeException(e); }
    }

    static final class Frame {
        String command;
        Map<String, String> headers = new HashMap<>();
        byte[] body;

        static String line(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\n') {
                if (c < 0) throw new EOFException();
                if (c != '\r') sb.append((char) c);
            }
            return sb.toString();
        }

        static Frame read(InputStream in) throws IOException {
            Frame f = new Frame();
            String cmd;
            do { cmd = line(in); } while (cmd.isEmpty()); // heart-beats
            f.command = cmd;
            for (String h; !(h = line(in)).isEmpty(); ) {
                int i = h.indexOf(':');
                f.headers.putIfAbsent(h.substring(0, i), h.substring(i + 1));
            }
            String len = f.headers.get("content-length");
            if (len != null) {
                f.body = in.readNBytes(Integer.parseInt(len));
                if (in.read() != 0) throw new IOException("no NUL");
            } else {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                for (int c; (c = in.read()) != 0; ) { if (c < 0) throw new EOFException(); b.write(c); }
                f.body = b.toByteArray();
            }
            return f;
        }
    }

    // ---- MQTT 3.1.1: QoS 1 sends at most 100 unacknowledged.
    static void writeLen(OutputStream out, int n) throws IOException {
        do {
            int d = n % 128;
            n /= 128;
            out.write(n > 0 ? d | 128 : d);
        } while (n > 0);
    }

    static void writeStr(ByteArrayOutputStream b, String s) {
        byte[] x = s.getBytes(StandardCharsets.UTF_8);
        b.write(x.length >> 8);
        b.write(x.length);
        b.write(x, 0, x.length);
    }

    static void packet(OutputStream out, int type, byte[] rest) throws IOException {
        out.write(type);
        writeLen(out, rest.length);
        out.write(rest);
    }

    static int readLen(InputStream in) throws IOException {
        int n = 0, mul = 1, d;
        do {
            d = in.read();
            if (d < 0) throw new EOFException();
            n += (d & 127) * mul;
            mul *= 128;
        } while ((d & 128) != 0);
        return n;
    }

    static void mqttConnect(OutputStream out, InputStream in, String id) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writeStr(b, "MQTT");
        b.write(4);
        b.write(0x80 | 0x40 | 0x02);
        b.write(0);
        b.write(60);
        writeStr(b, id);
        writeStr(b, "admin");
        writeStr(b, "admin");
        packet(out, 0x10, b.toByteArray());
        out.flush();
        int t = in.read();
        byte[] r = in.readNBytes(readLen(in));
        if (t != 0x20 || r[1] != 0) throw new IOException("CONNACK " + t + " " + Arrays.toString(r));
    }

    static void mqttConsumer(int qos) {
        try (Socket s = socket()) {
            InputStream in = new BufferedInputStream(s.getInputStream(), 65536);
            OutputStream out = new BufferedOutputStream(s.getOutputStream(), 65536);
            mqttConnect(out, in, "bench-consumer");
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            b.write(0);
            b.write(1);
            writeStr(b, "bench/mqtt");
            b.write(qos);
            packet(out, 0x82, b.toByteArray());
            out.flush();
            keepAlive(out, new byte[] {(byte) 0xc0, 0}, 30_000);
            byte[] ack = new byte[] {0x40, 2, 0, 0};
            while (!stop) {
                int t = in.read();
                if (t < 0) throw new EOFException();
                byte[] r = in.readNBytes(readLen(in));
                if ((t & 0xf0) != 0x30) continue;
                int tlen = ((r[0] & 0xff) << 8) | (r[1] & 0xff);
                int off = 2 + tlen;
                if (((t >> 1) & 3) > 0) {
                    ack[2] = r[off];
                    ack[3] = r[off + 1];
                    off += 2;
                    synchronized (out) {
                        out.write(ack);
                        if (in.available() == 0) out.flush();
                    }
                }
                record(r, off);
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static void mqttProducer(int qos, long rate) {
        try (Socket s = socket()) {
            InputStream in = new BufferedInputStream(s.getInputStream(), 65536);
            OutputStream out = new BufferedOutputStream(s.getOutputStream(), 65536);
            mqttConnect(out, in, "bench-producer");
            Semaphore window = new Semaphore(100);
            Thread acks = new Thread(wrap(() -> {
                try {
                    while (!stop) {
                        int t = in.read();
                        if (t < 0) throw new EOFException();
                        in.readNBytes(readLen(in));
                        if (t == 0x40) window.release();
                    }
                } catch (IOException e) { throw new UncheckedIOException(e); }
            }));
            acks.setDaemon(true);
            acks.start();
            byte[] topic = "bench/mqtt".getBytes(StandardCharsets.UTF_8);
            byte[] b = body();
            int rest = 2 + topic.length + (qos > 0 ? 2 : 0) + SIZE;
            Pacer pacer = new Pacer(rate, out);
            int id = 0;
            for (long i = 1; !stop; i++) {
                if (qos > 0) {
                    if (!window.tryAcquire()) { out.flush(); window.acquire(); }
                    id = id % 65535 + 1;
                }
                out.write(0x30 | (qos << 1));
                writeLen(out, rest);
                out.write(topic.length >> 8);
                out.write(topic.length);
                out.write(topic);
                if (qos > 0) { out.write(id >> 8); out.write(id); }
                stamp(b, 0);
                out.write(b);
                if (rate > 0 || i % 100 == 0) out.flush();
                sent.incrementAndGet();
                pacer.next();
            }
        } catch (IOException | InterruptedException e) { throw new RuntimeException(e); }
    }

    // ---- OpenWire through the ActiveMQ 5 JMS client,
    // producer window 1 MiB, auto-acknowledge.
    static javax.jms.Connection owConnection() throws javax.jms.JMSException {
        var f = new org.apache.activemq.ActiveMQConnectionFactory(
            "tcp://" + host + ":" + port + "?jms.producerWindowSize=1048576&wireFormat.tcpNoDelayEnabled=true");
        var c = f.createConnection("admin", "admin");
        c.start();
        return c;
    }

    static void owConsumer() {
        try {
            var c = owConnection();
            var s = c.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
            var consumer = s.createConsumer(s.createQueue("BENCH.OW"));
            while (!stop) {
                var m = (javax.jms.BytesMessage) consumer.receive(1000);
                if (m == null) continue;
                byte[] b = new byte[8];
                m.readBytes(b);
                record(b, 0);
            }
        } catch (javax.jms.JMSException e) { throw new RuntimeException(e); }
    }

    static void owProducer(long rate) {
        try {
            var c = owConnection();
            var s = c.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
            var p = s.createProducer(s.createQueue("BENCH.OW"));
            p.setDeliveryMode(PERSISTENT
                ? javax.jms.DeliveryMode.PERSISTENT : javax.jms.DeliveryMode.NON_PERSISTENT);
            byte[] b = body();
            Pacer pacer = new Pacer(rate);
            while (!stop) {
                var m = s.createBytesMessage();
                stamp(b, 0);
                m.writeBytes(b);
                p.send(m);
                sent.incrementAndGet();
                pacer.next();
            }
        } catch (javax.jms.JMSException e) { throw new RuntimeException(e); }
    }
}
