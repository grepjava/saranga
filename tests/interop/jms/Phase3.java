// Phase 3 interop suite: XA behaviour that Saranga must share with Artemis.
//   java -cp '<artemis>/lib/*' Phase3.java tcp://host:port [test-name...]
// Each test uses its own queues. Run it against the reference Java broker too:
// a test that fails there is a wrong test.

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;

public class Phase3 {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);
    static int xids;

    interface Test { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("xaSendTwoPhaseCommit", Phase3::xaSendTwoPhaseCommit);
        tests.put("xaSendRollback", Phase3::xaSendRollback);
        tests.put("xaAckOnePhaseCommit", Phase3::xaAckOnePhaseCommit);
        tests.put("xaAckRollbackRedelivers", Phase3::xaAckRollbackRedelivers);
        tests.put("xaPreparedAckIsHeld", Phase3::xaPreparedAckIsHeld);
        tests.put("xaRecover", Phase3::xaRecover);
        tests.put("xaErrors", Phase3::xaErrors);
        tests.put("xaSuspendResume", Phase3::xaSuspendResume);
        tests.put("xaWorkOutsideTransaction", Phase3::xaWorkOutsideTransaction);

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

    // ----------------------------------------------------------------- helpers

    static String name(String base) {
        return "p3." + base + "." + RUN;
    }

    static Xid xid() {
        xids++;
        return new XidImpl(("bq" + xids).getBytes(), 42, ("p3-" + RUN + "-" + xids).getBytes());
    }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    interface Body { void run(Connection plain, XASession xs, XAResource xa, Queue q) throws Exception; }

    /** A plain connection and an XA session, both on queue {@code base}. */
    static void with(String base, Body body) throws Exception {
        try (Connection plain = new ActiveMQConnectionFactory(url).createConnection();
             XAConnection xc = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
            plain.start();
            xc.start();
            XASession xs = xc.createXASession();
            Queue q = xs.createQueue(name(base));
            body.run(plain, xs, xs.getXAResource(), q);
        }
    }

    static void send(Connection c, Queue q, String... texts) throws Exception {
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageProducer p = s.createProducer(q);
            for (String t : texts) p.send(s.createTextMessage(t));
        }
    }

    static List<String> drain(Connection c, Queue q) throws Exception {
        List<String> got = new ArrayList<>();
        try (Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer mc = s.createConsumer(q);
            Message m;
            while ((m = mc.receive(500)) != null) got.add(((TextMessage) m).getText());
        }
        return got;
    }

    static int xaError(XAException e) {
        return e.errorCode;
    }

    // ------------------------------------------------------------------- tests

    static void xaSendTwoPhaseCommit() throws Exception {
        with("send2pc", (plain, xs, xa, q) -> {
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            MessageProducer p = xs.createProducer(q);
            p.send(xs.createTextMessage("a"));
            p.send(xs.createTextMessage("b"));
            xa.end(x, XAResource.TMSUCCESS);
            check(drain(plain, q).isEmpty(), "nothing before prepare");
            check(xa.prepare(x) == XAResource.XA_OK, "prepare is XA_OK");
            check(drain(plain, q).isEmpty(), "nothing before commit");
            xa.commit(x, false);
            check(drain(plain, q).equals(List.of("a", "b")), "both after commit");
        });
    }

    static void xaSendRollback() throws Exception {
        with("sendrb", (plain, xs, xa, q) -> {
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            xs.createProducer(q).send(xs.createTextMessage("a"));
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
            xa.rollback(x);
            check(drain(plain, q).isEmpty(), "rolled back send is dropped");
        });
    }

    static void xaAckOnePhaseCommit() throws Exception {
        with("ack1pc", (plain, xs, xa, q) -> {
            send(plain, q, "a", "b");
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            MessageConsumer mc = xs.createConsumer(q);
            check(mc.receive(2000) != null, "first");
            xa.end(x, XAResource.TMSUCCESS);
            xa.commit(x, true);
            mc.close();
            check(drain(plain, q).equals(List.of("b")), "only the unacked message remains");
        });
    }

    static void xaAckRollbackRedelivers() throws Exception {
        with("ackrb", (plain, xs, xa, q) -> {
            send(plain, q, "a");
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            MessageConsumer mc = xs.createConsumer(q);
            check(mc.receive(2000) != null, "received in xa");
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
            mc.close();
            xa.rollback(x);
            try (Session s = plain.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Message m = s.createConsumer(q).receive(2000);
                check(m != null, "redelivered after rollback");
                check(m.getJMSRedelivered(), "marked redelivered");
                check(m.getIntProperty("JMSXDeliveryCount") == 2, "delivery count 2, was " + m.getIntProperty("JMSXDeliveryCount"));
            }
        });
    }

    static void xaPreparedAckIsHeld() throws Exception {
        with("prepheld", (plain, xs, xa, q) -> {
            send(plain, q, "a");
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            MessageConsumer mc = xs.createConsumer(q);
            check(mc.receive(2000) != null, "received in xa");
            xa.end(x, XAResource.TMSUCCESS);
            xa.prepare(x);
            mc.close();
            check(drain(plain, q).isEmpty(), "a prepared ack holds the message");
            xa.commit(x, false);
            check(drain(plain, q).isEmpty(), "committed ack removes it");
        });
    }

    static void xaRecover() throws Exception {
        with("recover", (plain, xs, xa, q) -> {
            Xid prepared = xid();
            xa.start(prepared, XAResource.TMNOFLAGS);
            xs.createProducer(q).send(xs.createTextMessage("p"));
            xa.end(prepared, XAResource.TMSUCCESS);
            xa.prepare(prepared);
            Xid ended = xid();
            xa.start(ended, XAResource.TMNOFLAGS);
            xa.end(ended, XAResource.TMSUCCESS);
            List<Xid> inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(inDoubt.contains(prepared), "prepared xid is in doubt");
            check(!inDoubt.contains(ended), "an unprepared xid is not");
            xa.commit(prepared, false);
            xa.rollback(ended);
            inDoubt = Arrays.asList(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN));
            check(!inDoubt.contains(prepared), "committed xid is no longer in doubt");
            check(drain(plain, q).equals(List.of("p")), "committed send delivered");
        });
    }

    static void xaErrors() throws Exception {
        with("errors", (plain, xs, xa, q) -> {
            try {
                xa.commit(xid(), true);
                check(false, "commit of an unknown xid fails");
            } catch (XAException e) {
                check(xaError(e) == XAException.XAER_NOTA, "unknown xid: XAER_NOTA, was " + xaError(e));
            }
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            try {
                xa.prepare(x);
                check(false, "prepare while associated fails");
            } catch (XAException e) {
                check(xaError(e) == XAException.XAER_PROTO, "associated: XAER_PROTO, was " + xaError(e));
            }
            xa.end(x, XAResource.TMSUCCESS);
            try (XAConnection other = new ActiveMQXAConnectionFactory(url).createXAConnection()) {
                XAResource xa2 = other.createXASession().getXAResource();
                try {
                    xa2.start(x, XAResource.TMNOFLAGS);
                    check(false, "start of a known xid fails");
                } catch (XAException e) {
                    check(xaError(e) == XAException.XAER_DUPID, "known xid: XAER_DUPID, was " + xaError(e));
                }
            }
            try {
                xa.commit(x, false);
                check(false, "two-phase commit of an unprepared branch fails");
            } catch (XAException e) {
                // Artemis answers with ActiveMQIllegalStateException, which the
                // client reports as XAER_RMFAIL... or the code it maps to.
                check(e.errorCode != 0, "an error code");
            }
        });
    }

    static void xaSuspendResume() throws Exception {
        with("suspend", (plain, xs, xa, q) -> {
            Xid x = xid();
            xa.start(x, XAResource.TMNOFLAGS);
            MessageProducer p = xs.createProducer(q);
            p.send(xs.createTextMessage("in"));
            xa.end(x, XAResource.TMSUSPEND);
            xa.start(x, XAResource.TMRESUME);
            p.send(xs.createTextMessage("resumed"));
            xa.end(x, XAResource.TMSUCCESS);
            xa.commit(x, true);
            check(drain(plain, q).equals(List.of("in", "resumed")), "both sends committed");
        });
    }

    static void xaWorkOutsideTransaction() throws Exception {
        // An XA session with no branch started sends and acknowledges at once.
        with("outside", (plain, xs, xa, q) -> {
            xs.createProducer(q).send(xs.createTextMessage("now"));
            check(drain(plain, q).equals(List.of("now")), "delivered without a transaction");
        });
    }
}
