// Golden scenario "persist-crash": the broker is killed (SIGKILL) mid-transaction.
//   GOLDEN_KILL=1 scripts/golden.sh persist-crash
//   java -cp '<artemis>/lib/*' PersistCrash.java tcp://host:port
//
// Committed state: persist.crash gets crash-1..6 (crash-1 and crash-2 acked),
// the topic subscription persist.crash.topic::crash-client.sub gets one message,
// one message is scheduled for 2100, and crash-3 has been redelivered once.
// Then a local transaction sends crash-tx-1, crash-tx-2 and acks crash-3 and crash-4,
// without committing. The scenario prints READY and blocks; golden.sh then kills
// the broker. After recovery the open transaction must be gone: crash-3..6 are on
// the queue, crash-tx-* are not. ackBatchSize=0 and transactionBatchSize=0 send every
// ack at once (transacted sessions batch acks by transactionBatchSize), so the
// uncommitted acks reach the journal.

import javax.jms.*;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class PersistCrash {
    static final long SCHEDULED_AT = 4_102_444_800_000L;   // 2100-01-01T00:00:00Z

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?consumerWindowSize=0&ackBatchSize=0&transactionBatchSize=0");
        Connection conn = cf.createConnection();
        conn.setClientID("crash-client");
        conn.start();

        Session s = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
        Queue q = s.createQueue("persist.crash");
        Topic topic = s.createTopic("persist.crash.topic");
        s.createDurableSubscriber(topic, "sub").close();

        MessageProducer p = s.createProducer(q);
        for (int i = 1; i <= 6; i++) {
            TextMessage m = s.createTextMessage("crash-" + i);
            m.setIntProperty("seq", i);
            p.send(m);
        }
        TextMessage sched = s.createTextMessage("crash-scheduled");
        sched.setLongProperty("_AMQ_SCHED_DELIVERY", SCHEDULED_AT);
        p.send(sched);
        p.send(s.createTextMessage("crash-non-persistent"), DeliveryMode.NON_PERSISTENT, 4, 0);
        s.createProducer(topic).send(s.createTextMessage("crash-topic"));

        MessageConsumer c = s.createConsumer(q);
        for (int i = 0; i < 2; i++) {
            Message m = c.receive(5_000);
            require(m != null, "receive persist.crash");
            m.acknowledge();
        }
        c.close();

        // crash-3: one rollback, so a delivery count update is on disk.
        Session tx = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer tc = tx.createConsumer(q);
        require(tc.receive(5_000) != null, "receive crash-3");
        tx.rollback();

        // The open transaction: crash-3 (redelivered) and crash-4 acked, two sends, no commit.
        require(tc.receive(5_000) != null, "receive crash-3 again");
        require(tc.receive(5_000) != null, "receive crash-4");
        MessageProducer tp = tx.createProducer(q);
        tp.send(tx.createTextMessage("crash-tx-1"));
        tp.send(tx.createTextMessage("crash-tx-2"));

        System.out.println("READY");
        System.out.flush();
        Thread.sleep(600_000);   // golden.sh kills the broker, then this process
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
