// The SASL suite's client: Qpid JMS logging in to an AMQP acceptor that
// offers SCRAM-SHA-512, SCRAM-SHA-256 and PLAIN, with users whose SCRAM
// data come from SCRAMPropertiesLoginModule (alice: SCRAM-SHA-256 only;
// bob: both; carol: a password in clear; dave: no role), and to one that
// offers XOAUTH2, OAUTHBEARER and PLAIN, as tok, whose token is its
// password.
//
//   java -cp '<artemis>/lib/*' Suite.java host:scram-port host:token-port [test-name...]
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
      tokenPort = Integer.parseInt(args[1].split(":")[1]);
      Set<String> only = new HashSet<>(Arrays.asList(args).subList(2, args.length));
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
      // The token acceptor: XOAUTH2 through Qpid JMS, OAUTHBEARER (which
      // Qpid JMS lacks) over a raw socket. tok's token is its password.
      tests.put("xoauth2", () -> withPort(tokenPort, () -> roundTrip("tok", "secret-token", "XOAUTH2")));
      tests.put("xoauth2WrongToken", () -> withPort(tokenPort, () -> refused("tok", "wrong-token", "XOAUTH2")));
      tests.put("oauthBearer", () -> {
         check(saslOutcome("OAUTHBEARER", bearer("a=tok", "secret-token")) == 0, "outcome ok");
         check(saslOutcome("OAUTHBEARER", bearer("a=tok", "wrong-token")) == 1, "outcome auth for a wrong token");
         check(saslOutcome("OAUTHBEARER", bearer("", "secret-token")) == 1, "outcome auth without a user");
         check(saslOutcome("OAUTHBEARER", "n,a=tok,\u0001auth=Bearer secret-token\u0001") == 1,
               "outcome auth without the closing 0x01 0x01");
         check(saslOutcome("XOAUTH2", "user=tok\u0001auth=Bearer secret-token\u0001\u0001") == 0, "raw XOAUTH2");
      });
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

   static int tokenPort;

   static void withPort(int p, Test t) throws Exception {
      int saved = port;
      port = p;
      try {
         t.run();
      } finally {
         port = saved;
      }
   }

   /** RFC 7628's initial response, with the GS2 header's authzid. */
   static String bearer(String authzid, String token) {
      return "n," + authzid + ",\u0001host=" + host + "\u0001port=" + tokenPort + "\u0001auth=Bearer " + token + "\u0001\u0001";
   }

   /**
    * A SASL exchange on the token acceptor: the header, the mechanisms, a
    * sasl-init with this mechanism and initial response, and the outcome's
    * code.
    */
   static int saslOutcome(String mechanism, String initialResponse) throws Exception {
      try (java.net.Socket s = new java.net.Socket(host, tokenPort)) {
         s.setSoTimeout(5000);
         java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
         java.io.OutputStream out = s.getOutputStream();
         out.write(new byte[] {'A', 'M', 'Q', 'P', 3, 1, 0, 0});
         byte[] header = new byte[8];
         in.readFully(header);
         check(header[4] == 3, "the SASL header");
         readFrame(in);
         byte[] mech = mechanism.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
         byte[] resp = initialResponse.getBytes(java.nio.charset.StandardCharsets.UTF_8);
         java.io.ByteArrayOutputStream list = new java.io.ByteArrayOutputStream();
         java.io.DataOutputStream l = new java.io.DataOutputStream(list);
         l.writeInt(2);
         l.writeByte(0xb3);
         l.writeInt(mech.length);
         l.write(mech);
         l.writeByte(0xb0);
         l.writeInt(resp.length);
         l.write(resp);
         java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
         java.io.DataOutputStream b = new java.io.DataOutputStream(body);
         b.write(new byte[] {0x00, 0x53, 0x41, (byte) 0xd0});
         b.writeInt(list.size());
         b.write(list.toByteArray());
         java.io.DataOutputStream o = new java.io.DataOutputStream(out);
         o.writeInt(8 + body.size());
         o.write(new byte[] {2, 1, 0, 0});
         o.write(body.toByteArray());
         o.flush();
         byte[] outcome = readFrame(in);
         // The descriptor 0x00 0x53 0x44, a list, then the code as a ubyte.
         for (int i = 0; i + 2 < outcome.length; i++) {
            if (outcome[i] == 0x00 && outcome[i + 1] == 0x53 && outcome[i + 2] == 0x44) {
               for (int j = i + 3; j + 1 < outcome.length; j++) {
                  if (outcome[j] == 0x50) {
                     return outcome[j + 1];
                  }
               }
            }
         }
         throw new AssertionError("no sasl-outcome");
      }
   }

   /** A frame's body, after its 8-byte header. */
   static byte[] readFrame(java.io.DataInputStream in) throws Exception {
      int size = in.readInt();
      byte[] rest = new byte[size - 4];
      in.readFully(rest);
      return Arrays.copyOfRange(rest, 4, rest.length);
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
