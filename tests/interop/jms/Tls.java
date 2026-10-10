// Phase 6 TLS suite: TLS acceptors with each keystore type, masked store
// passwords, SNI, mutual TLS with certificate login, and revocation lists.
//   java -cp '<artemis>/lib/*' Tls.java <host> <tls-dir> <plain> <p12> <jks> <pem> <mtls> <crl> [test-name...]
// The ports are those of tests/interop/tls/broker.xml's acceptors; tls-dir
// holds tests/tls (the client's truststore and keystores). Run it with
// scripts/tls-interop.sh [saranga|artemis].

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class Tls {
    static String host, dir;
    static int plain, p12, jks, pem, mtls, crl;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run() throws Exception; }
    interface Action { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        host = args[0];
        dir = args[1].replace('\\', '/');
        plain = Integer.parseInt(args[2]);
        p12 = Integer.parseInt(args[3]);
        jks = Integer.parseInt(args[4]);
        pem = Integer.parseInt(args[5]);
        mtls = Integer.parseInt(args[6]);
        crl = Integer.parseInt(args[7]);
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(8, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("plainAcceptor", Tls::plainAcceptor);
        tests.put("pkcs12Keystore", Tls::pkcs12Keystore);
        tests.put("plainClientOnTls", Tls::plainClientOnTls);
        tests.put("untrustedServer", Tls::untrustedServer);
        tests.put("jksMaskedPassword", Tls::jksMaskedPassword);
        tests.put("pemKeystoreSni", Tls::pemKeystoreSni);
        tests.put("certificateLogin", Tls::certificateLogin);
        tests.put("clientCertificateRequired", Tls::clientCertificateRequired);
        tests.put("unknownCertificate", Tls::unknownCertificate);
        tests.put("revokedCertificate", Tls::revokedCertificate);

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

    static String trust() {
        return "trustStorePath=" + dir + "/truststore.p12;trustStorePassword=changeit;trustStoreType=PKCS12";
    }

    static String client(String name) {
        return "keyStorePath=" + dir + "/" + name + ".p12;keyStorePassword=changeit;keyStoreType=PKCS12";
    }

    static String url(int port, String params) {
        // No retries: a refused handshake should fail at once.
        return "tcp://" + host + ":" + port + "?reconnectAttempts=0;initialConnectAttempts=1"
            + (params.isEmpty() ? "" : ";" + params);
    }

    static Connection open(String url, String user, String password) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
        cf.setCallTimeout(5000);
        Connection c = user == null ? cf.createConnection() : cf.createConnection(user, password);
        c.start();
        return c;
    }

    /** Sends and receives one message; returns the validated user it carried. */
    static String roundTrip(String url, String user, String password) throws Exception {
        String q = "tls." + RUN + "." + Math.abs(url.hashCode());
        try (Connection c = open(url, user, password);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.Queue queue = s.createQueue(q);
            s.createProducer(queue).send(s.createTextMessage("hi"));
            Message m = s.createConsumer(queue).receive(5000);
            check(m != null && "hi".equals(((TextMessage) m).getText()), "round trip on " + url);
            return m.getStringProperty("_AMQ_VALIDATED_USER");
        }
    }

    static Exception fails(Action a) {
        try {
            a.run();
        } catch (Exception e) {
            return e;
        }
        throw new AssertionError("expected a failure");
    }

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static void eq(Object expected, Object actual, String what) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    // ------------------------------------------------------------------ tests

    static void plainAcceptor() throws Exception {
        eq("admin", roundTrip(url(plain, ""), "admin", "admin"), "validated user");
    }

    static void pkcs12Keystore() throws Exception {
        eq("admin", roundTrip(url(p12, "sslEnabled=true;" + trust()), "admin", "admin"), "validated user");
        // The password login still applies on a TLS acceptor.
        Exception e = fails(() -> open(url(p12, "sslEnabled=true;" + trust()), "admin", "nope").close());
        check(e instanceof JMSSecurityException, "wrong password refused: " + e);
    }

    static void plainClientOnTls() throws Exception {
        Exception e = fails(() -> open(url(p12, ""), "admin", "admin").close());
        check(!(e instanceof JMSSecurityException), "a plain client cannot speak to a TLS acceptor: " + e);
    }

    static void untrustedServer() throws Exception {
        // A client whose truststore lacks the CA refuses the server.
        String wrongTrust = "trustStorePath=" + dir + "/untrusted.p12;trustStorePassword=changeit;trustStoreType=PKCS12";
        fails(() -> open(url(p12, "sslEnabled=true;" + wrongTrust), "admin", "admin").close());
    }

    static void jksMaskedPassword() throws Exception {
        eq("admin", roundTrip(url(jks, "sslEnabled=true;" + trust()), "admin", "admin"), "validated user");
    }

    static void pemKeystoreSni() throws Exception {
        eq("admin", roundTrip(url(pem, "sslEnabled=true;" + trust()), "admin", "admin"), "localhost");
        eq("admin", roundTrip(url(pem, "sslEnabled=true;sniHost=localhost;" + trust()), "admin", "admin"), "sniHost=localhost");
        fails(() -> open(url(pem, "sslEnabled=true;sniHost=elsewhere.example;" + trust()), "admin", "admin").close());
    }

    static void certificateLogin() throws Exception {
        // No user name: the certificate is the login.
        eq("client", roundTrip(url(mtls, "sslEnabled=true;" + trust() + ";" + client("client")), null, null), "certificate user");
        // A user name given alongside does not matter to the certificate module.
        eq("client", roundTrip(url(mtls, "sslEnabled=true;" + trust() + ";" + client("client")), "admin", "x"), "certificate user");
    }

    static void clientCertificateRequired() throws Exception {
        Exception e = fails(() -> open(url(mtls, "sslEnabled=true;" + trust()), "admin", "admin").close());
        check(!(e instanceof JMSSecurityException), "no client certificate: no handshake: " + e);
    }

    static void unknownCertificate() throws Exception {
        // A trusted certificate the users file does not list.
        Exception e = fails(() -> open(url(mtls, "sslEnabled=true;" + trust() + ";" + client("other")), null, null).close());
        check(e instanceof JMSSecurityException, "refused login: " + e);
        String m = e.getMessage();
        check(m.startsWith("AMQ229031: Unable to validate user from "), m);
        check(m.endsWith(". Username: null; SSL certificate subject DN: CN=other, OU=Test, O=Saranga"), m);
    }

    static void revokedCertificate() throws Exception {
        eq("client", roundTrip(url(crl, "sslEnabled=true;" + trust() + ";" + client("client")), null, null), "unrevoked");
        Exception e = fails(() -> open(url(crl, "sslEnabled=true;" + trust() + ";" + client("other")), null, null).close());
        check(!(e instanceof JMSSecurityException), "a revoked certificate gets no handshake: " + e);
    }
}
