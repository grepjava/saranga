// The GSSAPI suite's client: Qpid JMS logging in with GSSAPI, its Kerberos
// credentials from a keytab through a JAAS realm of client-login.config
// (sasl.options.configScope), and with PLAIN against a realm that checks the
// password with the KDC.
//
//   java -cp '<artemis>/lib/*' -Djava.security.krb5.conf=... \
//       -Djava.security.auth.login.config=client-login.config Suite.java \
//       gss-port unbound-port other-port nokey-port password-port ldap-port ldaps-port [test-name...]
//
// Run by tests/interop/gssapi/gssapi.sh [saranga|artemis].

import java.util.*;
import javax.jms.*;
import javax.jms.Queue;

public class Suite {
   interface Test { void run() throws Exception; }

   static void check(boolean ok, String what) {
      if (!ok) {
         throw new AssertionError(what);
      }
   }

   public static void main(String[] args) throws Exception {
      int gss = Integer.parseInt(args[0]);
      int unbound = Integer.parseInt(args[1]);
      int other = Integer.parseInt(args[2]);
      int nokey = Integer.parseInt(args[3]);
      int password = Integer.parseInt(args[4]);
      int ldap = Integer.parseInt(args[5]);
      int ldaps = Integer.parseInt(args[6]);
      Set<String> only = new HashSet<>(Arrays.asList(args).subList(7, args.length));

      Map<String, Test> tests = new LinkedHashMap<>();
      tests.put("gssapi", () -> works(gssapi(gss, "client-keytab"), null, null));
      tests.put("gssapiUnbound", () -> works(gssapi(unbound, "client-keytab"), null, null));
      tests.put("gssapiOtherPrincipal", () -> refused(gssapi(other, "client-keytab"), null, null));
      tests.put("gssapiNoStoredKey", () -> refused(gssapi(nokey, "client-keytab"), null, null));
      tests.put("gssapiNoRole", () -> noRole(gssapi(gss, "norole-keytab")));
      tests.put("gssapiPlainRefused", () -> {
         try (Connection c = plain(gss).createConnection("pwuser", "pw-secret")) {
            c.start();
            throw new AssertionError("logged in");
         } catch (JMSSecurityException expected) {
            check(String.valueOf(expected.getMessage()).contains("SASL"), expected.getMessage());
         }
      });
      tests.put("kerberosPassword", () -> works(plain(password), "pwuser", "pw-secret"));
      tests.put("kerberosWrongPassword", () -> refused(plain(password), "pwuser", "wrong"));
      tests.put("kerberosUnknownUser", () -> refused(plain(password), "nobody", "pw-secret"));
      tests.put("ldapGssapiBind", () -> works(gssapi(ldap, "ldapclient-keytab"), null, null, "ldapq."));
      tests.put("ldapsGssapiBind", () -> works(gssapi(ldaps, "ldapsclient-keytab"), null, null, "ldapq."));
      tests.put("ldapGssapiBindNoGroup", () -> noRole(gssapi(ldap, "ldapother-keytab"), "ldapq.none"));

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

   static ConnectionFactory gssapi(int port, String scope) {
      return new org.apache.qpid.jms.JmsConnectionFactory("amqp://localhost:" + port
            + "?amqp.saslMechanisms=GSSAPI&sasl.options.configScope=" + scope);
   }

   static ConnectionFactory plain(int port) {
      return new org.apache.qpid.jms.JmsConnectionFactory("amqp://localhost:" + port + "?amqp.saslMechanisms=PLAIN");
   }

   static int queues;

   static void works(ConnectionFactory f, String user, String password) throws Exception {
      works(f, user, password, "gssapi.q");
   }

   static void works(ConnectionFactory f, String user, String password, String prefix) throws Exception {
      try (Connection c = f.createConnection(user, password)) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue(prefix + (++queues));
         MessageConsumer consumer = s.createConsumer(q);
         s.createProducer(q).send(s.createTextMessage("hi"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("hi"), "received " + got);
      }
   }

   static void noRole(ConnectionFactory f) throws Exception {
      noRole(f, "gssapi.none");
   }

   static void noRole(ConnectionFactory f, String address) throws Exception {
      try (Connection c = f.createConnection()) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         try {
            s.createProducer(s.createQueue(address)).send(s.createTextMessage("x"));
            throw new AssertionError("sent without a role");
         } catch (JMSException expected) {
            check(String.valueOf(expected.getMessage()).contains("does not have permission"), String.valueOf(expected.getMessage()));
         }
      }
   }

   static void refused(ConnectionFactory f, String user, String password) throws Exception {
      try (Connection c = f.createConnection(user, password)) {
         c.start();
         throw new AssertionError("logged in");
      } catch (JMSSecurityException expected) {
      }
   }
}
