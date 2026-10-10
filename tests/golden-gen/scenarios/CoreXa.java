// Golden scenario "core-xa": XA transactions through the Artemis JMS client.
//   java -cp '<artemis>/lib/*' CoreXa.java tcp://host:port
// Leaves one prepared (in-doubt) transaction so the journal holds a PREPARE.

import javax.jms.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

public class CoreXa {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        try (ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(url);
             XAConnection conn = cf.createXAConnection()) {
            conn.start();
            XASession xs = conn.createXASession();
            XAResource xa = xs.getXAResource();
            Session s = xs.getSession();
            Queue q = s.createQueue("golden.xa");
            MessageProducer p = s.createProducer(q);

            require(xa.setTransactionTimeout(120), "set timeout");
            xa.getTransactionTimeout();

            // Two-phase commit.
            Xid x1 = xid(1);
            xa.start(x1, XAResource.TMNOFLAGS);
            p.send(s.createTextMessage("xa-committed"));
            xa.end(x1, XAResource.TMSUCCESS);
            require(xa.prepare(x1) == XAResource.XA_OK, "prepare x1");
            xa.commit(x1, false);

            // One-phase commit.
            Xid x2 = xid(2);
            xa.start(x2, XAResource.TMNOFLAGS);
            p.send(s.createTextMessage("xa-one-phase"));
            xa.end(x2, XAResource.TMSUCCESS);
            xa.commit(x2, true);

            // Rollback after prepare.
            Xid x3 = xid(3);
            xa.start(x3, XAResource.TMNOFLAGS);
            p.send(s.createTextMessage("xa-rolled-back"));
            xa.end(x3, XAResource.TMSUCCESS);
            xa.prepare(x3);
            xa.rollback(x3);

            // Suspend and resume.
            Xid x4 = xid(4);
            xa.start(x4, XAResource.TMNOFLAGS);
            xa.end(x4, XAResource.TMSUSPEND);
            xa.start(x4, XAResource.TMRESUME);
            MessageConsumer c = s.createConsumer(q);
            require(c.receive(5_000) != null, "receive in x4");
            xa.end(x4, XAResource.TMSUCCESS);
            xa.commit(x4, true);
            c.close();

            // Leave one prepared: an in-doubt transaction for the journal and for recover().
            Xid x5 = xid(5);
            xa.start(x5, XAResource.TMNOFLAGS);
            p.send(s.createTextMessage("xa-in-doubt"));
            xa.end(x5, XAResource.TMSUCCESS);
            xa.prepare(x5);
            Xid[] inDoubt = xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            require(inDoubt.length >= 1, "recover lists the prepared xid");
        }
        System.out.println("core-xa: ok");
    }

    static Xid xid(int n) {
        return new XidImpl(("branch-" + n).getBytes(), 0x4a5458 + n, ("global-transaction-" + n).getBytes());
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
