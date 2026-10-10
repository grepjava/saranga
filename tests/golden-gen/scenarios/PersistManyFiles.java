// Golden scenario "persist-many-files": enough traffic to roll, reclaim and compact the journal.
//   java -cp '<artemis>/lib/*' PersistManyFiles.java tcp://host:port
//
// Runs with tests/golden/etc/persist-many-files.broker.xml (1 MiB journal files,
// journal-compact-min-files 3, journal-compact-percentage 50). Sends 35 batches of
// 100 durable 10 KiB messages (about 35 MB) to persist.bulk and after each batch
// consumes the 99 with keep = false. The 35 messages with keep = true (seq 0, 100,
// ..., 3400) stay, one per batch, spread over the journal's files. A last small
// queue, persist.bulk.tail, gets 3 messages after the bulk traffic.

import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class PersistManyFiles {
    static final int BATCHES = 35;
    static final int BATCH = 100;
    static final int SIZE = 10 * 1024;

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
             Connection conn = cf.createConnection()) {
            conn.start();
            Session tx = conn.createSession(true, Session.SESSION_TRANSACTED);
            Queue q = tx.createQueue("persist.bulk");
            MessageProducer p = tx.createProducer(q);
            MessageConsumer c = tx.createConsumer(q, "keep = false");

            int seq = 0;
            for (int b = 0; b < BATCHES; b++) {
                for (int i = 0; i < BATCH; i++, seq++) {
                    BytesMessage m = tx.createBytesMessage();
                    m.writeBytes(payload(seq));
                    m.setIntProperty("seq", seq);
                    m.setBooleanProperty("keep", seq % BATCH == 0);
                    p.send(m);
                }
                tx.commit();
                for (int i = 0; i < BATCH - 1; i++) {
                    require(c.receive(10_000) != null, "receive batch " + b);
                }
                tx.commit();
            }
            c.close();

            Queue tail = tx.createQueue("persist.bulk.tail");
            MessageProducer tp = tx.createProducer(tail);
            for (int i = 1; i <= 3; i++) {
                tp.send(tx.createTextMessage("tail-" + i));
            }
            tx.commit();
        }
        System.out.println("persist-many-files: ok");
    }

    static byte[] payload(int seq) {
        byte[] b = new byte[SIZE];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (seq + i * 7);
        }
        return b;
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
