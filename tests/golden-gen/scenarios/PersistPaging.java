// Golden scenario "persist-paging": paged addresses at a clean shutdown.
//   java -cp '<artemis>/lib/*' PersistPaging.java tcp://host:port
// Runs with tests/golden/etc/persist-paging.broker.xml: golden.paging.# pages above
// 100 KiB in 20 KiB page files.
//
// Paging directory: page files of a queue partly consumed (cursor acks, completed and
// deleted pages), a topic with two durable subscriptions consumed unevenly, committed
// and rolled-back transactions (page transactions), a prepared XA branch, large
// messages on a paging address, and non-persistent messages that were paged.
// Bodies and properties are fixed; IDs and timestamps vary between runs.

import javax.jms.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

public class PersistPaging {
    static final String PAD = "x".repeat(1000);

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?consumerWindowSize=0");
             Connection conn = cf.createConnection()) {
            conn.setClientID("paging-client");
            conn.start();
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // A queue: 400 messages, the first 150 consumed.
            Queue q = s.createQueue("golden.paging.queue");
            send(s, q, "queue", 400, DeliveryMode.PERSISTENT);
            consume(s, q, 150);

            // A topic with two durable subscriptions: one consumes 80 of 200.
            Topic t = s.createTopic("golden.paging.topic");
            s.createDurableSubscriber(t, "sub-a").close();
            s.createDurableSubscriber(t, "sub-b").close();
            send(s, t, "topic", 200, DeliveryMode.PERSISTENT);
            try (TopicSubscriber sub = s.createDurableSubscriber(t, "sub-a")) {
                for (int i = 0; i < 80; i++) {
                    require(sub.receive(5000) != null, "sub-a message " + i);
                }
            }

            // Transactions on a paging address: one committed, one rolled back.
            Queue tq = s.createQueue("golden.paging.tx");
            send(s, tq, "tx-fill", 120, DeliveryMode.PERSISTENT);
            Session tx = conn.createSession(true, Session.SESSION_TRANSACTED);
            send(tx, tq, "tx-committed", 60, DeliveryMode.PERSISTENT);
            tx.commit();
            send(tx, tq, "tx-rolledback", 40, DeliveryMode.PERSISTENT);
            tx.rollback();
            tx.close();

            // Large messages on a paging address.
            Queue lq = s.createQueue("golden.paging.large");
            send(s, lq, "large-fill", 120, DeliveryMode.PERSISTENT);
            MessageProducer lp = s.createProducer(lq);
            for (int i = 0; i < 3; i++) {
                BytesMessage m = s.createBytesMessage();
                byte[] body = new byte[200_000 + i];
                for (int j = 0; j < body.length; j++) body[j] = (byte) (j * 31 + i);
                m.writeBytes(body);
                m.setIntProperty("i", i);
                lp.send(m);
            }

            // Non-persistent messages that were paged.
            Queue np = s.createQueue("golden.paging.np");
            send(s, np, "np", 150, DeliveryMode.NON_PERSISTENT);
        }

        // A prepared XA branch that sent to a paging address.
        try (XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            xc.start();
            XASession xs = xc.createXASession();
            Queue xq = xs.createQueue("golden.paging.xa");
            send(xs, xq, "xa-fill", 120, DeliveryMode.PERSISTENT);
            XAResource xa = xs.getXAResource();
            Xid x = new XidImpl("paging-bq".getBytes(), 7, "paging-gtrid".getBytes());
            xa.start(x, XAResource.TMNOFLAGS);
            send(xs, xq, "xa-prepared", 30, DeliveryMode.PERSISTENT);
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
        }
        System.out.println("persist-paging: ok");
    }

    static void send(Session s, Destination d, String prefix, int n, int mode) throws Exception {
        MessageProducer p = s.createProducer(d);
        p.setDeliveryMode(mode);
        for (int i = 0; i < n; i++) {
            TextMessage m = s.createTextMessage(prefix + "-" + i + "-" + PAD);
            m.setIntProperty("i", i);
            p.send(m);
        }
        p.close();
    }

    static void consume(Session s, Destination d, int n) throws Exception {
        try (MessageConsumer c = s.createConsumer(d)) {
            for (int i = 0; i < n; i++) {
                Message m = c.receive(5000);
                require(m != null && m.getIntProperty("i") == i, "message " + i + " in order");
            }
        }
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
