// The SASL suite's client: Qpid JMS logging in to an AMQP acceptor that
// offers SCRAM-SHA-512, SCRAM-SHA-256 and PLAIN, with users whose SCRAM
// data come from SCRAMPropertiesLoginModule (alice: SCRAM-SHA-256 only;
// bob: both; carol: a password in clear; dave: no role).
//
//   java -cp '<artemis>/lib/*' Suite.java host:port [test-name...]
//
// Run by tests/interop/sasl/sasl.sh [saranga|artemis].

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

public class Suite {
   static String host;
   static int port;

   interface Test { void run() throws Exception; }

   static void check(boolean ok, String what) {
      if (!ok) {
         throw new AssertionError(what);
      }
   }

   public static void main(String[] args) throws Exception {
      String[] hp = args[0].split(":");
      host = hp[0];
      port = Integer.parseInt(hp[1]);
      Set<String> only = new HashSet<>(Arrays.asList(args).subList(1, args.length));
      Map<String, Test> tests = new LinkedHashMap<>();
      tests.put("scramSha256", () -> roundTrip("alice", "alicepw", "SCRAM-SHA-256"));
      tests.put("scramSha512", () -> roundTrip("bob", "bobpw", "SCRAM-SHA-512"));
      tests.put("negotiated", () -> roundTrip("bob", "bobpw", null));
      tests.put("clearPassword", () -> {
         roundTrip("carol", "carolpw", "SCRAM-SHA-256");
         roundTrip("carol", "carolpw", "SCRAM-SHA-512");
      });
      tests.put("wrongPassword", () -> refused("alice", "wrong", "SCRAM-SHA-256"));
      tests.put("noDataForMechanism", () -> refused("alice", "alicepw", "SCRAM-SHA-512"));
      tests.put("unknownUser", () -> refused("nobody", "x", "SCRAM-SHA-256"));
      tests.put("noRole", Suite::noRole);
      tests.put("plain", () -> roundTrip("artemis", "artemis", "PLAIN"));
      tests.put("plainIsNotScram", () -> refused("alice", "alicepw", "PLAIN"));
      int failed = 0;
      for (Map.Entry<String, Test> t : tests.entrySet()) {
         if (!only.isEmpty() && !only.contains(t.getKey())) {
            continue;
         }
         try {
            t.getValue().run();
            System.out.println("PASS " + t.getKey());
         } catch (Throwable e) {
            failed++;
            System.out.println("FAIL " + t.getKey() + ": " + e);
            for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
               System.out.println("  caused by " + c);
            }
         }
      }
      System.exit(failed == 0 ? 0 : 1);
   }

   static ConnectionFactory factory(String mechanism) {
      String uri = "amqp://" + host + ":" + port;
      if (mechanism != null) {
         uri += "?amqp.saslMechanisms=" + mechanism;
      }
      return new org.apache.qpid.jms.JmsConnectionFactory(uri);
   }

   /** A message sent and received on a queue of its own. */
   static void roundTrip(String user, String password, String mechanism) throws Exception {
      try (Connection c = factory(mechanism).createConnection(user, password)) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue("sasl." + user);
         MessageConsumer consumer = s.createConsumer(q);
         s.createProducer(q).send(s.createTextMessage("from " + user));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("from " + user), "received " + got);
      }
   }

   static void refused(String user, String password, String mechanism) throws Exception {
      try (Connection c = factory(mechanism).createConnection(user, password)) {
         c.start();
         throw new AssertionError(user + " logged in with " + mechanism);
      } catch (JMSSecurityException expected) {
      }
   }

   /**
    * dave logs in, and may not make the queue he sends to: the producer is
    * refused with amqp:internal-error, which is how Artemis reports a
    * security exception from auto-creating a sender's address.
    */
   static void noRole() throws Exception {
      try (Connection c = factory("SCRAM-SHA-256").createConnection("dave", "davepw")) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         try {
            s.createProducer(s.createQueue("sasl.dave")).send(s.createTextMessage("x"));
            throw new AssertionError("dave sent");
         } catch (JMSException expected) {
            check(!(expected instanceof JMSSecurityException), "a security exception: " + expected);
            String m = String.valueOf(expected.getMessage());
            check(m.contains("User: dave does not have permission='CREATE_ADDRESS' on address sasl.dave"), m);
            check(m.contains("amqp:internal-error"), m);
         }
      }
   }
}
