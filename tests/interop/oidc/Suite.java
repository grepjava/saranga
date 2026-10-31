// The OIDC suite's client and OpenID Connect provider. The provider serves
// /.well-known/openid-configuration and its JWK set over HTTP and HTTPS;
// tokens are signed here with its keys (RSA, and EC on P-256, P-384 and
// P-521), or with one it does not publish, and handed to Qpid JMS as the
// PLAIN password.
//
//   java -cp '<artemis>/lib/*' Suite.java host oidc-port plain-port tls-port http-port https-port provider.p12 [test-name...]
//
// Run by tests/interop/oidc/oidc.sh [saranga|artemis].

import com.sun.net.httpserver.*;
import java.io.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.*;
import java.security.spec.*;
import java.util.*;
import javax.jms.*;
import javax.jms.Queue;
import javax.net.ssl.*;

public class Suite {
   static String host;
   static int oidcPort, plainPort, tlsPort, httpPort, httpsPort;

   interface Test { void run() throws Exception; }

   static void check(boolean ok, String what) {
      if (!ok) {
         throw new AssertionError(what);
      }
   }

   static KeyPair rsa, rogue, ec256, ec384, ec521;

   public static void main(String[] args) throws Exception {
      host = args[0];
      oidcPort = Integer.parseInt(args[1]);
      plainPort = Integer.parseInt(args[2]);
      tlsPort = Integer.parseInt(args[3]);
      httpPort = Integer.parseInt(args[4]);
      httpsPort = Integer.parseInt(args[5]);
      Set<String> only = new HashSet<>(Arrays.asList(args).subList(7, args.length));

      KeyPairGenerator r = KeyPairGenerator.getInstance("RSA");
      r.initialize(2048);
      rsa = r.generateKeyPair();
      rogue = r.generateKeyPair();
      ec256 = ec("secp256r1");
      ec384 = ec("secp384r1");
      ec521 = ec("secp521r1");
      serve(HttpServer.create(new InetSocketAddress("localhost", httpPort), 0), "http://localhost:" + httpPort);
      HttpsServer https = HttpsServer.create(new InetSocketAddress("localhost", httpsPort), 0);
      https.setHttpsConfigurator(new HttpsConfigurator(tls(args[6])));
      serve(https, "https://localhost:" + httpsPort);

      Map<String, Test> tests = new LinkedHashMap<>();
      tests.put("rs256", () -> works(oidcPort, sign("RS256", "rsa", rsa, claims())));
      tests.put("rs512", () -> works(oidcPort, sign("RS512", "rsa", rsa, claims())));
      tests.put("ps256", () -> works(oidcPort, sign("PS256", "rsa", rsa, claims())));
      tests.put("ps384", () -> works(oidcPort, sign("PS384", "rsa", rsa, claims())));
      tests.put("es256", () -> works(oidcPort, sign("ES256", "ec256", ec256, claims())));
      tests.put("es384", () -> works(oidcPort, sign("ES384", "ec384", ec384, claims())));
      tests.put("es512", () -> works(oidcPort, sign("ES512", "ec521", ec521, claims())));
      tests.put("wrongCurve", () -> refused(oidcPort, sign("ES256", "ec384", ec384, claims())));
      tests.put("expired", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().with("exp", now() - 120))));
      tests.put("expiredWithinSkew", () -> works(oidcPort, sign("RS256", "rsa", rsa, claims().with("exp", now() - 30))));
      tests.put("notYet", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().with("nbf", now() + 120))));
      tests.put("notYetWithinSkew", () -> works(oidcPort, sign("RS256", "rsa", rsa, claims().with("nbf", now() + 30))));
      tests.put("wrongAudience", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().with("aud", "other"))));
      tests.put("audienceList", () -> works(oidcPort, sign("RS256", "rsa", rsa,
            claims().withRaw("aud", "[\"other\",\"artemis\"]"))));
      tests.put("missingAzp", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().without("azp"))));
      tests.put("badClaimType", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().with("exp", "soon"))));
      tests.put("tampered", () -> {
         String t = sign("RS256", "rsa", rsa, claims());
         String[] p = t.split("\\.");
         String other = b64(claims().with("preferred_username", "mallory").json().getBytes(StandardCharsets.UTF_8));
         refused(oidcPort, p[0] + "." + other + "." + p[2]);
      });
      tests.put("unknownKey", () -> refused(oidcPort, sign("RS256", "rogue", rogue, claims())));
      tests.put("wrongKid", () -> refused(oidcPort, sign("RS256", "ec256", rsa, claims())));
      tests.put("noKid", () -> works(oidcPort, sign("RS256", null, rsa, claims())));
      tests.put("hs256", () -> {
         javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
         mac.init(new javax.crypto.spec.SecretKeySpec("0123456789abcdef0123456789abcdef".getBytes(), "HmacSHA256"));
         String in = b64("{\"alg\":\"HS256\"}".getBytes()) + "." + b64(claims().json().getBytes(StandardCharsets.UTF_8));
         refused(oidcPort, in + "." + b64(mac.doFinal(in.getBytes(StandardCharsets.US_ASCII))));
      });
      tests.put("typJwtLowerCase", () -> works(oidcPort, sign("RS256", "rsa", rsa, claims(), "jwt")));
      tests.put("typAtJwt", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims(), "at+jwt")));
      tests.put("critical", () -> refused(oidcPort, signHeader("RS256", rsa,
            "{\"alg\":\"RS256\",\"kid\":\"rsa\",\"crit\":[\"exp\"],\"exp\":1}", claims())));
      tests.put("plainRefused", () -> refused(oidcPort, plain(claims())));
      tests.put("plainAllowed", () -> works(plainPort, plain(new Claims().with("sub", "x").withRaw("roles", "[\"amq\"]"))));
      tests.put("encrypted", () -> refused(oidcPort, b64("{\"alg\":\"RSA-OAEP\",\"enc\":\"A128GCM\"}".getBytes()) + ".a.b.c.d"));
      tests.put("notAJwt", () -> refused(oidcPort, "a-password"));
      tests.put("noRole", () -> {
         String t = sign("RS256", "rsa", rsa, claims().withRaw("realm_access", "{\"roles\":[\"nothing\"]}"));
         try (Connection c = factory(oidcPort).createConnection("x", t)) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
               s.createProducer(s.createQueue("oidc.none")).send(s.createTextMessage("x"));
               throw new AssertionError("sent without a role");
            } catch (JMSException expected) {
               check(String.valueOf(expected.getMessage()).contains("does not have permission"), String.valueOf(expected.getMessage()));
            }
         }
      });
      tests.put("missingIdentity", () -> refused(oidcPort, sign("RS256", "rsa", rsa, claims().without("preferred_username"))));
      tests.put("thumbprintWithoutTls", () -> refused(plainPort, sign("RS256", "rsa", rsa,
            new Claims().with("sub", "x").withRaw("roles", "[\"amq\"]").withRaw("cnf", "{\"x5t#256\":\"abc\"}"))));
      tests.put("https", () -> works(tlsPort, sign("RS256", "rsa", rsa, new Claims().with("sub", "x").withRaw("roles", "[\"amq\"]"))));

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

   // ------------------------------------------------------- the provider

   static KeyPair ec(String curve) throws Exception {
      KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
      g.initialize(new ECGenParameterSpec(curve));
      return g.generateKeyPair();
   }

   static SSLContext tls(String p12) throws Exception {
      KeyStore ks = KeyStore.getInstance("PKCS12");
      try (InputStream in = new FileInputStream(p12)) {
         ks.load(in, "changeit".toCharArray());
      }
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, "changeit".toCharArray());
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(kmf.getKeyManagers(), null, null);
      return ctx;
   }

   static void serve(HttpServer server, String issuer) {
      server.createContext("/.well-known/openid-configuration", ex -> reply(ex,
            "{\"issuer\":\"" + issuer + "\",\"jwks_uri\":\"" + issuer + "/keys\"}"));
      server.createContext("/keys", ex -> reply(ex, jwks()));
      server.start();
   }

   static void reply(HttpExchange ex, String body) throws IOException {
      byte[] b = body.getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().add("Content-Type", "application/json");
      ex.sendResponseHeaders(200, b.length);
      try (OutputStream o = ex.getResponseBody()) {
         o.write(b);
      }
   }

   static String unsigned(BigInteger i, int size) {
      byte[] b = i.toByteArray();
      if (b.length > 1 && b[0] == 0) {
         b = Arrays.copyOfRange(b, 1, b.length);
      }
      if (size > b.length) {
         byte[] p = new byte[size];
         System.arraycopy(b, 0, p, size - b.length, b.length);
         b = p;
      }
      return b64(b);
   }

   static String ecJwk(String kid, String crv, KeyPair k, int size) {
      ECPoint w = ((ECPublicKey) k.getPublic()).getW();
      return "{\"kty\":\"EC\",\"kid\":\"" + kid + "\",\"use\":\"sig\",\"crv\":\"" + crv + "\",\"x\":\""
            + unsigned(w.getAffineX(), size) + "\",\"y\":\"" + unsigned(w.getAffineY(), size) + "\"}";
   }

   static String jwks() {
      RSAPublicKey p = (RSAPublicKey) rsa.getPublic();
      return "{\"keys\":["
            + "{\"kty\":\"RSA\",\"kid\":\"rsa\",\"use\":\"sig\",\"n\":\"" + unsigned(p.getModulus(), 0)
            + "\",\"e\":\"" + unsigned(p.getPublicExponent(), 0) + "\"},"
            + ecJwk("ec256", "P-256", ec256, 32) + ","
            + ecJwk("ec384", "P-384", ec384, 48) + ","
            + ecJwk("ec521", "P-521", ec521, 66) + ","
            + "{\"kty\":\"oct\",\"kid\":\"secret\",\"k\":\"c2VjcmV0\"}"
            + "]}";
   }

   // ------------------------------------------------------- tokens

   static String b64(byte[] b) {
      return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
   }

   static long now() {
      return System.currentTimeMillis() / 1000;
   }

   /** Claims, in order, values as JSON. */
   static class Claims {
      final LinkedHashMap<String, String> m = new LinkedHashMap<>();

      Claims with(String k, String v) {
         m.put(k, "\"" + v + "\"");
         return this;
      }

      Claims with(String k, long v) {
         m.put(k, Long.toString(v));
         return this;
      }

      Claims withRaw(String k, String json) {
         m.put(k, json);
         return this;
      }

      Claims without(String k) {
         m.remove(k);
         return this;
      }

      String json() {
         StringBuilder b = new StringBuilder("{");
         for (Map.Entry<String, String> e : m.entrySet()) {
            if (b.length() > 1) {
               b.append(',');
            }
            b.append('"').append(e.getKey()).append("\":").append(e.getValue());
         }
         return b.append('}').toString();
      }
   }

   /** What the oidc realm requires: aud, iss, sub, azp and exp. */
   static Claims claims() {
      return new Claims().with("iss", "http://localhost:" + httpPort).with("sub", "subject-1")
            .with("aud", "artemis").with("azp", "suite").with("exp", now() + 300).with("iat", now())
            .with("preferred_username", "alice").withRaw("realm_access", "{\"roles\":[\"broker-user\"]}");
   }

   static String sign(String alg, String kid, KeyPair key, Claims claims) throws Exception {
      return sign(alg, kid, key, claims, "JWT");
   }

   static String sign(String alg, String kid, KeyPair key, Claims claims, String typ) throws Exception {
      String header = "{\"alg\":\"" + alg + "\",\"typ\":\"" + typ + "\"" + (kid == null ? "" : ",\"kid\":\"" + kid + "\"") + "}";
      return signHeader(alg, key, header, claims);
   }

   static String signHeader(String alg, KeyPair key, String header, Claims claims) throws Exception {
      String in = b64(header.getBytes(StandardCharsets.UTF_8)) + "." + b64(claims.json().getBytes(StandardCharsets.UTF_8));
      Signature s;
      String bits = alg.substring(2);
      switch (alg.substring(0, 2)) {
         case "RS" -> s = Signature.getInstance("SHA" + bits + "withRSA");
         case "PS" -> {
            s = Signature.getInstance("RSASSA-PSS");
            int len = Integer.parseInt(bits) / 8;
            s.setParameter(new PSSParameterSpec("SHA-" + bits, "MGF1", new MGF1ParameterSpec("SHA-" + bits), len, 1));
         }
         case "ES" -> s = Signature.getInstance("SHA" + bits + "withECDSAinP1363Format");
         default -> throw new IllegalArgumentException(alg);
      }
      s.initSign(key.getPrivate());
      s.update(in.getBytes(StandardCharsets.US_ASCII));
      return in + "." + b64(s.sign());
   }

   static String plain(Claims claims) {
      return b64("{\"alg\":\"none\"}".getBytes()) + "." + b64(claims.json().getBytes(StandardCharsets.UTF_8)) + ".";
   }

   // ------------------------------------------------------- the client

   static ConnectionFactory factory(int port) {
      return new org.apache.qpid.jms.JmsConnectionFactory("amqp://" + host + ":" + port + "?amqp.saslMechanisms=PLAIN");
   }

   static void works(int port, String token) throws Exception {
      try (Connection c = factory(port).createConnection("x", token)) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue("oidc.q");
         MessageConsumer consumer = s.createConsumer(q);
         s.createProducer(q).send(s.createTextMessage("hi"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("hi"), "received " + got);
      }
   }

   static void refused(int port, String token) throws Exception {
      try (Connection c = factory(port).createConnection("x", token)) {
         c.start();
         throw new AssertionError("logged in");
      } catch (JMSSecurityException expected) {
      }
   }
}
