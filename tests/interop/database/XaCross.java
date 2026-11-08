// Mixed XA: one broker prepares, is killed, and the other finishes.
//   java -cp "<artemis>/lib/*" XaCross.java seed|prepare|finish|check [url]
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.jms.*;
import javax.transaction.xa.*;
import org.apache.activemq.artemis.jms.client.*;

public class XaCross {
   static String URL = "tcp://localhost:61616";
   static final String USER = "admin", PW = "admin", Q = "xa.q", DUP = "_AMQ_DUPL_ID";
   static final int LARGE = 300_000;

   static Xid xid(int n) {
      byte[] g = ("xa-cross-" + n).getBytes(), b = ("branch-" + n).getBytes();
      return new Xid() {
         public int getFormatId() { return 77; }
         public byte[] getGlobalTransactionId() { return g; }
         public byte[] getBranchQualifier() { return b; }
      };
   }

   static void require(boolean c, String m) {
      if (!c) {
         System.out.println("XACROSS FAIL " + m);
         System.exit(1);
      }
   }

   public static void main(String[] a) throws Exception {
      if (a.length > 1) {
         URL = a[1];
      }
      URL += (URL.contains("?") ? "&" : "?") + "minLargeMessageSize=100000";
      switch (a[0]) {
         case "seed" -> seed();
         case "prepare" -> prepare();
         case "finish" -> finish();
         case "check" -> check();
         default -> require(false, "mode");
      }
      System.out.println("XACROSS " + a[0] + " ok");
      System.exit(0);
   }

   static void seed() throws Exception {
      try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(URL); Connection c = cf.createConnection(USER, PW)) {
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue(Q);
         MessageProducer p = s.createProducer(q);
         for (int i = 1; i <= 6; i++) {
            TextMessage m = s.createTextMessage("seed-" + i);
            if (i >= 5) {
               m.setStringProperty(DUP, "d" + (i - 4));   // d1, d2 fill the cache of two
            }
            p.send(m);
         }
      }
   }

   static void prepare() throws Exception {
      try (ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(URL)) {
         XAConnection c = cf.createXAConnection(USER, PW);
         c.start();
         XASession xs = c.createXASession();
         XAResource xa = xs.getXAResource();
         Session s = xs.getSession();
         Queue q = s.createQueue(Q);
         MessageProducer p = s.createProducer(q);
         MessageConsumer r = s.createConsumer(q);

         Xid x1 = xid(1);                           // commits: a send, an ack, a duplicate ID
         xa.start(x1, XAResource.TMNOFLAGS);
         TextMessage m = s.createTextMessage("x1-new");
         m.setStringProperty(DUP, "d3");            // takes d1's place, so a record is deleted
         p.send(m);
         require("seed-1".equals(((TextMessage) r.receive(5000)).getText()), "x1 receive");
         xa.end(x1, XAResource.TMSUCCESS);
         require(xa.prepare(x1) == XAResource.XA_OK, "prepare x1");

         Xid x2 = xid(2);                           // rolls back: the same, undone
         xa.start(x2, XAResource.TMNOFLAGS);
         p.send(s.createTextMessage("x2-new"));
         require("seed-2".equals(((TextMessage) r.receive(5000)).getText()), "x2 receive");
         xa.end(x2, XAResource.TMSUCCESS);
         require(xa.prepare(x2) == XAResource.XA_OK, "prepare x2");

         Xid x3 = xid(3);                           // commits: a large message
         xa.start(x3, XAResource.TMNOFLAGS);
         BytesMessage b = s.createBytesMessage();
         byte[] body = new byte[LARGE];
         Arrays.fill(body, (byte) 7);
         b.writeBytes(body);
         b.setStringProperty("name", "x3-large");
         p.send(b);
         xa.end(x3, XAResource.TMSUCCESS);
         require(xa.prepare(x3) == XAResource.XA_OK, "prepare x3");
      }
   }

   static void finish() throws Exception {
      try (ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(URL)) {
         XAConnection c = cf.createXAConnection(USER, PW);
         XAResource xa = c.createXASession().getXAResource();
         Xid[] found = xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
         Set<String> names = new TreeSet<>();
         for (Xid x : found) {
            String g = new String(x.getGlobalTransactionId());
            require(x.getFormatId() == 77 && new String(x.getBranchQualifier()).equals("branch-" + g.substring(9)), "xid " + g);
            names.add(g);
         }
         System.out.println("recovered " + names);
         require(names.equals(new TreeSet<>(List.of("xa-cross-1", "xa-cross-2", "xa-cross-3"))), "recovered " + names);
         xa.commit(xid(1), false);
         xa.rollback(xid(2));
         xa.commit(xid(3), false);
         require(xa.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length == 0, "left in doubt");
      }
   }

   static void check() throws Exception {
      try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(URL); Connection c = cf.createConnection(USER, PW)) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue(Q);
         MessageProducer p = s.createProducer(q);
         TextMessage again = s.createTextMessage("d3-again");   // its ID committed with x1
         again.setStringProperty(DUP, "d3");
         try {
            p.send(again);
         } catch (JMSException e) {
            System.out.println("duplicate refused: " + e.getMessage());
         }
         MessageConsumer r = s.createConsumer(q);
         Set<String> got = new TreeSet<>();
         for (Message m; (m = r.receive(4000)) != null; ) {
            if (m instanceof TextMessage t) {
               got.add(t.getText());
            } else {
               BytesMessage b = (BytesMessage) m;
               require(b.getBodyLength() == LARGE, "large size " + b.getBodyLength());
               byte[] body = new byte[LARGE];
               b.readBytes(body);
               for (byte v : body) {
                  require(v == 7, "large body");
               }
               got.add(m.getStringProperty("name"));
            }
         }
         System.out.println("consumed " + got);
         Set<String> want = new TreeSet<>(List.of("seed-2", "seed-3", "seed-4", "seed-5", "seed-6", "x1-new", "x3-large"));
         require(got.equals(want), "wanted " + want);
      }
   }
}
