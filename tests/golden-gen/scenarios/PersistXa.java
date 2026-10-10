// Golden scenario "persist-xa": XA transactions in every state at a clean shutdown.
//   java -cp '<artemis>/lib/*' PersistXa.java tcp://host:port
//
// persist.xa starts with 6 committed messages (xa-seed-1..6). Then:
//   x1: send 2 + ack 1, prepared, left in doubt
//   x2: send 1 + ack 1, prepared, committed
//   x3: send 1 + ack 1, prepared, rolled back (the acked message is redelivered later)
//   x4: send 1 to persist.xa.other + ack 1, prepared, left in doubt
//   x5: send 1, one-phase commit
//   x6: send 1, rolled back without prepare
// The journal keeps two PREPARE records; recover() lists x1 and x4.

import javax.jms.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

public class PersistXa {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        // No prefetch, so each receive takes exactly one message from the queue.
        try (ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(url + "?consumerWindowSize=0");
             XAConnection conn = cf.createXAConnection()) {
            conn.start();

            Session plain = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = plain.createQueue("persist.xa");
            Queue other = plain.createQueue("persist.xa.other");
            MessageProducer seed = plain.createProducer(q);
            for (int i = 1; i <= 6; i++) {
                seed.send(plain.createTextMessage("xa-seed-" + i));
            }
            plain.createConsumer(other).close();   // make sure persist.xa.other exists

            XASession xs = conn.createXASession();
            XAResource xa = xs.getXAResource();
            Session s = xs.getSession();
            MessageProducer p = s.createProducer(null);
            require(xa.setTransactionTimeout(3600), "set timeout");

            Xid x1 = xid(1);
            xa.start(x1, XAResource.TMNOFLAGS);
            p.send(q, text(s, "x1-send-1"));
            p.send(q, text(s, "x1-send-2"));
            receiveOne(s, q, "x1");
            xa.end(x1, XAResource.TMSUCCESS);
            require(xa.prepare(x1) == XAResource.XA_OK, "prepare x1");

            Xid x2 = xid(2);
            xa.start(x2, XAResource.TMNOFLAGS);
            p.send(q, text(s, "x2-send-1"));
            receiveOne(s, q, "x2");
            xa.end(x2, XAResource.TMSUCCESS);
            require(xa.prepare(x2) == XAResource.XA_OK, "prepare x2");
            xa.commit(x2, false);

            Xid x3 = xid(3);
            xa.start(x3, XAResource.TMNOFLAGS);
            p.send(q, text(s, "x3-send-1"));
            receiveOne(s, q, "x3");
            xa.end(x3, XAResource.TMSUCCESS);
            require(xa.prepare(x3) == XAResource.XA_OK, "prepare x3");
            xa.rollback(x3);

            Xid x4 = xid(4);
            xa.start(x4, XAResource.TMNOFLAGS);
            p.send(other, text(s, "x4-send-other"));
            receiveOne(s, q, "x4");
            xa.end(x4, XAResource.TMSUCCESS);
            require(xa.prepare(x4) == XAResource.XA_OK, "prepare x4");

            Xid x5 = xid(5);
            xa.start(x5, XAResource.TMNOFLAGS);
            p.send(q, text(s, "x5-one-phase"));
            xa.end(x5, XAResource.TMSUCCESS);
            xa.commit(x5, true);

            Xid x6 = xid(6);
            xa.start(x6, XAResource.TMNOFLAGS);
            p.send(q, text(s, "x6-rolled-back"));
            xa.end(x6, XAResource.TMSUCCESS);
            xa.rollback(x6);

            Xid[] inDoubt = xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            require(inDoubt.length == 2, "recover lists x1 and x4, got " + inDoubt.length);
        }
        System.out.println("persist-xa: ok");
    }

    static TextMessage text(Session s, String body) throws JMSException {
        TextMessage m = s.createTextMessage(body);
        m.setStringProperty("tag", body);
        return m;
    }

    // The consumer is closed inside the branch, so it cannot hold a second message.
    static void receiveOne(Session s, Queue q, String what) throws JMSException {
        MessageConsumer c = s.createConsumer(q);
        require(c.receive(5_000) != null, "receive in " + what);
        c.close();
    }

    static Xid xid(int n) {
        return new XidImpl(("persist-branch-" + n).getBytes(), 0x50455253, ("persist-global-" + n).getBytes());
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
