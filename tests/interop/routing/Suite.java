// The connection router suite's client: the Artemis JMS client (Core), Qpid
// JMS (AMQP), the ActiveMQ client (OpenWire) and raw MQTT 3.1.1 and 5
// packets against the router broker's acceptors, and the routers asked
// through management messages and Jolokia.
//
//   java -cp '<artemis>/lib/*' Suite.java phase r-port t1-port t2-port http-port broker.xml [test-name...]
//
// phase: main, cache-prime (t1 down) or cache-check (after r restarted).
// Run by tests/interop/routing/routing.sh [saranga|artemis].

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import javax.jms.*;
import javax.jms.Queue;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;

public class Suite {
   interface Test { void run() throws Exception; }

   static int r, t1, t2, http;
   static Path xml;
   static String t1Node, t2Node, rNode;

   static void check(boolean ok, String what) {
      if (!ok) {
         throw new AssertionError(what);
      }
   }

   public static void main(String[] args) throws Exception {
      String phase = args[0];
      r = Integer.parseInt(args[1]);
      t1 = Integer.parseInt(args[2]);
      t2 = Integer.parseInt(args[3]);
      http = Integer.parseInt(args[4]);
      xml = Paths.get(args[5]);
      Set<String> only = new HashSet<>(Arrays.asList(args).subList(6, args.length));
      rNode = (String) attribute(r, "broker", "NodeID");
      // t1 is down while the cache is primed.
      t1Node = nodeId(t1);
      t2Node = nodeId(t2);

      Map<String, Test> tests = new LinkedHashMap<>();
      switch (phase) {
         case "main" -> main(tests);
         case "cache-prime" -> {
            // t1 is down: once its monitor says so, both route to t2.
            tests.put("cachePrime", () -> {
               check(t2Node.equals(awaitNode("cached", "sticky", t2Node)), "cached sticky");
               check(t2Node.equals(awaitNode("volatile", "sticky", t2Node)), "volatile sticky");
            });
         }
         case "cache-check" -> {
            tests.put("cacheCheck", () -> {
               // t1 is ready again once a new key goes to it.
               awaitNode("volatile", "fresh", t1Node);
               // Every pool's monitors have checked both targets.
               Thread.sleep(3000);
               String cached = node(target("cached", "sticky"));
               check(t2Node.equals(cached), "the persisted cache keeps t2, not " + cached + " (t1 " + t1Node + ")");
               check(t1Node.equals(node(target("volatile", "sticky"))), "the volatile cache forgot t2");
            });
         }
         default -> throw new IllegalArgumentException(phase);
      }

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

   static void main(Map<String, Test> tests) {
      int redirect = r + 1, local = r + 2, notReady = r + 3, role = r + 4, sni = r + 5, ip = r + 6, missing = r + 7;

      // Redirected to t1: the message lands there.
      tests.put("coreRedirect", () -> landsOn(core(redirect, null), "artemis", "artemis", "routing.core", t1));
      tests.put("amqpRedirect", () -> landsOn(amqpFailover(redirect, null), "artemis", "artemis", "routing.amqp", t1));
      tests.put("openwireRedirect", () -> landsOn(openwire(redirect, null), "artemis", "artemis", "routing.openwire", t1));
      tests.put("mqtt5Redirect", () -> {
         Mqtt m = mqtt(redirect, 5, "m5", "artemis", "artemis");
         check(m.type == 2 && m.code == 0x9C, "CONNACK USE_ANOTHER_SERVER: " + m);
         check(("localhost:" + t1).equals(m.serverReference), "server reference: " + m);
      });
      tests.put("mqtt311Redirect", () -> {
         Mqtt m = mqtt(redirect, 4, "m3", "artemis", "artemis");
         check(m.type == 2 && m.code == 0x9C && m.serverReference == null, "CONNACK 0x9C: " + m);
      });

      // A local client ID stays; any other is refused.
      tests.put("coreLocal", () -> landsOn(core(local, "local-core"), "artemis", "artemis", "routing.local", r));
      tests.put("coreRefused", () -> refused(core(local, "elsewhere"), "AMQ229240: Connection router local-only rejected the connection"));
      tests.put("amqpLocal", () -> landsOn(amqp(local, "local-amqp"), "artemis", "artemis", "routing.local.amqp", r));
      tests.put("amqpRefused", () -> refused(amqp(local, "elsewhere"), "Connection router local-only rejected this connection"));
      // A refused OpenWire connection is closed with no answer: the
      // client's failover transport keeps the start waiting.
      tests.put("openwireRefused", () -> {
         Connection c = openwire(local, "elsewhere").createConnection("artemis", "artemis");
         Thread start = new Thread(() -> {
            try {
               c.start();
            } catch (JMSException ignored) {
            }
         });
         start.setDaemon(true);
         start.start();
         start.join(5000);
         check(start.isAlive(), "the connection was answered");
         try {
            c.close();
         } catch (JMSException ignored) {
         }
      });
      tests.put("openwireNotFaultTolerant", () -> refused(new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:" + local + "?jms.clientID=local-ow"), "Client not fault tolerant"));
      tests.put("mqtt5Refused", () -> {
         Mqtt m = mqtt(local, 5, "elsewhere", "artemis", "artemis");
         check(m.type == 2 && m.code == 0x9C && m.serverReference == null, "CONNACK USE_ANOTHER_SERVER: " + m);
      });
      tests.put("mqtt5Local", () -> {
         Mqtt m = mqtt(local, 5, "local-mqtt", "artemis", "artemis");
         check(m.type == 2 && m.code == 0, "CONNACK SUCCESS: " + m);
      });

      // A pool with nothing ready.
      tests.put("amqpNotReady", () -> refused(amqp(notReady, "x"), "Connection router not-ready is not ready"));
      tests.put("mqtt5NotReady", () -> {
         Mqtt m = mqtt(notReady, 5, "x", "artemis", "artemis");
         check(m.type == 2 && m.code == 0x88, "CONNACK SERVER_UNAVAILABLE: " + m);
      });
      tests.put("mqtt311NotReady", () -> {
         Mqtt m = mqtt(notReady, 4, "x", "artemis", "artemis");
         check(m.type == 2 && m.code == 0x88, "CONNACK 0x88: " + m);
      });

      // An acceptor naming a router there is none of.
      tests.put("coreMissing", () -> refused(core(missing, "x"), null));
      tests.put("amqpMissing", () -> refused(amqp(missing, "x"), "Unrecoverable error"));
      tests.put("mqtt5Missing", () -> {
         Mqtt m = mqtt(missing, 5, "x", "artemis", "artemis");
         check(m.type == 14 && m.code == 0x83, "DISCONNECT IMPLEMENTATION_SPECIFIC_ERROR: " + m);
      });

      // By role: local-role stays, remote-role goes to t1.
      tests.put("roleLocal", () -> landsOn(core(role, null), "localuser", "local-secret", "routing.role.local", r));
      tests.put("roleRedirect", () -> landsOn(core(role, null), "remoteuser", "remote-secret", "routing.role.remote", t1));

      // By SNI host and by source address.
      tests.put("sniLocal", () -> landsOn(coreTls(sni, "local.example"), "artemis", "artemis", "routing.sni", r));
      tests.put("sniRefused", () -> refused(coreTls(sni, "other.example"), "AMQ229240: Connection router by-sni rejected the connection"));
      tests.put("sourceIp", () -> landsOn(new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(
         "tcp://127.0.0.1:" + ip + "?ha=true&reconnectAttempts=10&retryInterval=250"), "artemis", "artemis", "routing.ip", r));

      // Management.
      tests.put("targetAsJSON", () -> {
         Map<String, Object> t = awaitTarget("hash", "some-key");
         String node = (String) t.get("nodeID");
         check(node.equals(t1Node) || node.equals(t2Node), "a target: " + t);
         check(Boolean.FALSE.equals(t.get("local")), "not local: " + t);
         Map<?, ?> connector = (Map<?, ?>) t.get("connector");
         String name = (String) connector.get("name");
         Map<?, ?> params = (Map<?, ?>) connector.get("params");
         check(name.equals(node.equals(t1Node) ? "t1" : "t2"), "the connector: " + t);
         check(String.valueOf(node.equals(t1Node) ? t1 : t2).equals(params.get("port")), "its port: " + t);
         check(t.equals(target("hash", "some-key")), "the same again");
      });
      tests.put("localTargetEnabled", () -> {
         Map<String, Object> t = awaitTarget("local-enabled", "any");
         check(rNode.equals(t.get("nodeID")) && Boolean.TRUE.equals(t.get("local")) && t.get("connector") == null, "local: " + t);
      });
      tests.put("modulo", () -> {
         String zero = null, one = null;
         for (int i = 0; zero == null || one == null; i++) {
            String k = "client-" + i;
            if (((fnv(k) & Integer.MAX_VALUE) % 2) == 0) {
               zero = zero == null ? k : zero;
            } else {
               one = one == null ? k : one;
            }
         }
         Map<String, Object> t = target("modulo", zero);
         check(t != null && Boolean.TRUE.equals(t.get("local")), zero + " is local: " + t);
         check(target("modulo", one) == null, one + " goes nowhere");
         check(target("modulo", "NULL") != null, "NULL is local");
      });
      tests.put("quorum", () -> {
         Thread.sleep(1500);
         check(target("quorum", "q") == null, "no quorum");
      });
      tests.put("leastConnections", () -> {
         awaitTarget("least", "warm");
         long c1 = ((Number) attribute(t1, "broker", "ConnectionCount")).longValue();
         long c2 = ((Number) attribute(t2, "broker", "ConnectionCount")).longValue();
         int port = c1 <= c2 ? t1 : t2;
         String busy = c1 <= c2 ? t1Node : t2Node, idle = c1 <= c2 ? t2Node : t1Node;
         List<Connection> held = new ArrayList<>();
         try {
            for (long i = 0; i <= Math.abs(c1 - c2) + 3; i++) {
               Connection c = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + port).createConnection("artemis", "artemis");
               c.start();
               held.add(c);
            }
            check(idle.equals(awaitNode("least", "busy", idle)), "the least busy: not " + busy);
         } finally {
            for (Connection c : held) {
               c.close();
            }
         }
      });
      tests.put("clusterPool", () -> {
         String n = awaitNode("cluster", "c-key", null);
         check(n.equals(t1Node) || n.equals(t2Node), "a member: " + n);
      });
      tests.put("discoveryPool", () -> {
         String n = awaitNode("discovery", "d-key", null);
         check(n.equals(t1Node) || n.equals(t2Node), "a broadcaster: " + n);
      });
      tests.put("filters", () -> {
         check("local-.*".equals(attribute(r, "connectionrouter.local-only", "LocalTargetFilter")), "LocalTargetFilter");
         check(".*-role".equals(attribute(r, "connectionrouter.by-role", "TargetKeyFilter")), "TargetKeyFilter");
         check(attribute(r, "connectionrouter.local-only", "TargetKeyFilter") == null, "no TargetKeyFilter");
         invoke(r, "connectionrouter.local-only", "setLocalTargetFilter", "elsewhere");
         try {
            check("elsewhere".equals(attribute(r, "connectionrouter.local-only", "LocalTargetFilter")), "set");
            check(Boolean.TRUE.equals(target("local-only", "elsewhere").get("local")), "now local");
         } finally {
            invoke(r, "connectionrouter.local-only", "setLocalTargetFilter", "local-.*");
         }
         invoke(r, "connectionrouter.by-role", "setTargetKeyFilter", (Object) null);
         check(attribute(r, "connectionrouter.by-role", "TargetKeyFilter") == null, "cleared");
         invoke(r, "connectionrouter.by-role", "setTargetKeyFilter", ".*-role");
      });
      tests.put("jolokiaGetTarget", () -> {
         Map<String, Object> want = awaitTarget("hash", "jolokia-key");
         String search = jolokia("{\"type\":\"search\",\"mbean\":\"org.apache.activemq.artemis:component=connection-routers,*\"}");
         java.util.regex.Matcher found = java.util.regex.Pattern
            .compile("org\\.apache\\.activemq\\.artemis:broker=\\\\\"[^\\\\]*\\\\\",component=connection-routers,name=\\\\\"hash\\\\\"").matcher(search.substring(search.indexOf("\"value\"")));
         check(found.find(), "the hash router's MBean: " + search);
         String mbean = found.group().replace("\\\"", "\"");
         String body = "{\"type\":\"exec\",\"mbean\":\"" + mbean.replace("\\", "\\\\").replace("\"", "\\\"")
            + "\",\"operation\":\"getTarget(java.lang.String)\",\"arguments\":[\"jolokia-key\"]}";
         String got = jolokia(body);
         check(got.contains("\"nodeID\":\"" + want.get("nodeID") + "\""), "nodeID: " + got);
         check(got.contains("\"local\":false"), "local: " + got);
         check(got.contains("\"port\":\"" + (want.get("nodeID").equals(t1Node) ? t1 : t2) + "\""), "the port: " + got);
      });
      tests.put("reload", () -> {
         String text = Files.readString(xml);
         Files.writeString(xml, text.replace("<local-target-filter>local-.*</local-target-filter>",
            "<local-target-filter>reloaded-.*</local-target-filter>"));
         try {
            long deadline = System.currentTimeMillis() + 20000;
            while (!"reloaded-.*".equals(attribute(r, "connectionrouter.local-only", "LocalTargetFilter"))) {
               check(System.currentTimeMillis() < deadline, "not reloaded");
               Thread.sleep(250);
            }
            landsOn(core(local, "reloaded-core"), "artemis", "artemis", "routing.reloaded", r);
         } finally {
            Files.writeString(xml, text);
         }
      });
   }

   // ------------------------------------------------------------- clients

   static ConnectionFactory core(int port, String clientId) {
      return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + port
         + "?ha=true&reconnectAttempts=10&retryInterval=250&initialConnectAttempts=1"
         + (clientId != null ? "&clientID=" + clientId : ""));
   }

   static ConnectionFactory coreTls(int port, String sniHost) {
      return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + port
         + "?sslEnabled=true&trustAll=true&sniHost=" + sniHost + "&reconnectAttempts=0");
   }

   static ConnectionFactory amqp(int port, String clientId) {
      return new org.apache.qpid.jms.JmsConnectionFactory("amqp://localhost:" + port
         + (clientId != null ? "?jms.clientID=" + clientId : ""));
   }

   static ConnectionFactory amqpFailover(int port, String clientId) {
      return new org.apache.qpid.jms.JmsConnectionFactory("failover:(amqp://localhost:" + port
         + ")?failover.maxReconnectAttempts=10" + (clientId != null ? "&jms.clientID=" + clientId : ""));
   }

   static ConnectionFactory openwire(int port, String clientId) {
      return new org.apache.activemq.ActiveMQConnectionFactory("failover:(tcp://localhost:" + port
         + ")?randomize=false&startupMaxReconnectAttempts=2&maxReconnectAttempts=2&initialReconnectDelay=100"
         + (clientId != null ? "&jms.clientID=" + clientId : ""));
   }

   static int queues;

   /** Sends through the factory, then receives from the broker at {@code port} directly. */
   static void landsOn(ConnectionFactory f, String user, String password, String prefix, int port) throws Exception {
      String queue = prefix + "." + (++queues) + "." + System.nanoTime();
      try (Connection c = f.createConnection(user, password)) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         s.createProducer(s.createQueue(queue)).send(s.createTextMessage("routed"));
      }
      try (Connection c = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + port).createConnection("artemis", "artemis")) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TextMessage got = (TextMessage) s.createConsumer(s.createQueue(queue)).receive(5000);
         check(got != null && got.getText().equals("routed"), "on " + port + ": " + got);
      }
   }

   static void refused(ConnectionFactory f, String message) throws Exception {
      try (Connection c = f.createConnection("artemis", "artemis")) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         s.createProducer(s.createQueue("routing.refused")).send(s.createTextMessage("x"));
         throw new AssertionError("connected");
      } catch (JMSException expected) {
         StringBuilder all = new StringBuilder();
         for (Throwable t = expected; t != null; t = t.getCause()) {
            all.append(t).append(" | ");
         }
         check(message == null || all.toString().contains(message), "the error: " + all);
      }
   }

   // ---------------------------------------------------------------- MQTT

   record Mqtt(int type, int code, String serverReference) {}

   /** A CONNECT, and the first packet back: its type and code, with a v5 CONNACK's server reference. */
   static Mqtt mqtt(int port, int version, String clientId, String user, String password) throws IOException {
      ByteArrayOutputStream body = new ByteArrayOutputStream();
      DataOutputStream d = new DataOutputStream(body);
      d.writeUTF("MQTT");
      d.writeByte(version);
      d.writeByte(0x80 | 0x40 | 0x02);
      d.writeShort(60);
      if (version == 5) {
         d.writeByte(0);
      }
      d.writeUTF(clientId);
      d.writeUTF(user);
      d.writeUTF(password);
      try (Socket s = new Socket("localhost", port)) {
         s.setSoTimeout(10000);
         OutputStream out = s.getOutputStream();
         out.write(0x10);
         writeVarInt(out, body.size());
         body.writeTo(out);
         out.flush();
         DataInputStream in = new DataInputStream(s.getInputStream());
         int first = in.read();
         if (first < 0) {
            return new Mqtt(-1, -1, null);
         }
         byte[] packet = new byte[readVarInt(in)];
         in.readFully(packet);
         int type = first >> 4;
         if (type == 14) {
            return new Mqtt(type, packet.length > 0 ? packet[0] & 0xFF : 0, null);
         }
         String reference = null;
         if (version == 5 && packet.length > 2) {
            DataInputStream p = new DataInputStream(new ByteArrayInputStream(packet, 2, packet.length - 2));
            int length = readVarInt(p);
            byte[] props = new byte[length];
            p.readFully(props);
            DataInputStream q = new DataInputStream(new ByteArrayInputStream(props));
            while (q.available() > 0) {
               int id = q.read();
               if (id == 0x1C) {
                  reference = q.readUTF();
               } else {
                  break;
               }
            }
         }
         return new Mqtt(type, packet[1] & 0xFF, reference);
      }
   }

   static void writeVarInt(OutputStream out, int v) throws IOException {
      do {
         int b = v & 0x7F;
         v >>>= 7;
         out.write(v > 0 ? b | 0x80 : b);
      } while (v > 0);
   }

   static int readVarInt(InputStream in) throws IOException {
      int v = 0, shift = 0, b;
      do {
         b = in.read();
         v |= (b & 0x7F) << shift;
         shift += 7;
      } while ((b & 0x80) != 0);
      return v;
   }

   // ---------------------------------------------------------- management

   static Object request(int port, java.util.function.Consumer<Message> fill) throws Exception {
      try (QueueConnection c = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + port).createQueueConnection("artemis", "artemis")) {
         c.start();
         QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         QueueRequestor requestor = new QueueRequestor(s, ActiveMQJMSClient.createQueue("activemq.management"));
         Message m = s.createMessage();
         fill.accept(m);
         Message reply = requestor.request(m);
         if (!JMSManagementHelper.hasOperationSucceeded(reply)) {
            throw new AssertionError("management: " + JMSManagementHelper.getResult(reply));
         }
         return JMSManagementHelper.getResult(reply);
      }
   }

   static String nodeId(int port) {
      try {
         return (String) attribute(port, "broker", "NodeID");
      } catch (Exception e) {
         return null;
      }
   }

   static Object attribute(int port, String resource, String attribute) throws Exception {
      return request(port, m -> {
         try {
            JMSManagementHelper.putAttribute(m, resource, attribute);
         } catch (JMSException e) {
            throw new RuntimeException(e);
         }
      });
   }

   static Object invoke(int port, String resource, String operation, Object... params) throws Exception {
      return request(port, m -> {
         try {
            JMSManagementHelper.putOperationInvocation(m, resource, operation, params);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
   }

   @SuppressWarnings("unchecked")
   static Map<String, Object> target(String router, String key) throws Exception {
      Object json = invoke(r, "connectionrouter." + router, "getTargetAsJSON", key);
      if (json == null) {
         return null;
      }
      return (Map<String, Object>) parse(new StringReader((String) json));
   }

   static String node(Map<String, Object> t) {
      return t == null ? null : (String) t.get("nodeID");
   }

   /** The router's target for a key once it has one (the pool's monitors are ready). */
   static Map<String, Object> awaitTarget(String router, String key) throws Exception {
      long deadline = System.currentTimeMillis() + 20000;
      while (true) {
         Map<String, Object> t = target(router, key);
         if (t != null) {
            return t;
         }
         check(System.currentTimeMillis() < deadline, router + " has no target for " + key);
         Thread.sleep(250);
      }
   }

   /** The node the router sends the key to once it is {@code want} (any, if null). */
   static String awaitNode(String router, String key, String want) throws Exception {
      long deadline = System.currentTimeMillis() + 40000;
      while (true) {
         String n = node(target(router, key));
         if (n != null && (want == null || want.equals(n))) {
            return n;
         }
         check(System.currentTimeMillis() < deadline, router + " sends " + key + " to " + n + ", not " + want);
         Thread.sleep(250);
      }
   }

   static String jolokia(String body) throws Exception {
      HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + http + "/console/jolokia/"))
         .header("Authorization", "Basic " + Base64.getEncoder().encodeToString("artemis:artemis".getBytes(StandardCharsets.UTF_8)))
         .header("Origin", "http://localhost")
         .header("Content-Type", "application/json")
         .POST(HttpRequest.BodyPublishers.ofString(body)).build();
      HttpResponse<String> res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
      check(res.statusCode() == 200 && res.body().contains("\"status\":200"), "Jolokia: " + res.statusCode() + " " + res.body());
      return res.body();
   }

   static int fnv(String s) {
      int h = 0x811c9dc5;
      for (int i = 0; i < s.length(); i++) {
         h = (h ^ s.charAt(i)) * 0x01000193;
      }
      return h;
   }

   // ---------------------------------------------------- a little JSON

   static Object parse(Reader r) throws IOException {
      PushbackReader in = new PushbackReader(r, 1);
      return value(in);
   }

   static int skip(PushbackReader in) throws IOException {
      int c;
      do {
         c = in.read();
      } while (Character.isWhitespace(c));
      return c;
   }

   static Object value(PushbackReader in) throws IOException {
      int c = skip(in);
      switch (c) {
         case '{' -> {
            Map<String, Object> m = new LinkedHashMap<>();
            c = skip(in);
            if (c == '}') {
               return m;
            }
            in.unread(c);
            while (true) {
               skip(in);
               String k = string(in);
               skip(in);
               m.put(k, value(in));
               c = skip(in);
               if (c == '}') {
                  return m;
               }
            }
         }
         case '[' -> {
            List<Object> l = new ArrayList<>();
            c = skip(in);
            if (c == ']') {
               return l;
            }
            in.unread(c);
            while (true) {
               l.add(value(in));
               c = skip(in);
               if (c == ']') {
                  return l;
               }
            }
         }
         case '"' -> {
            return string(in);
         }
         default -> {
            StringBuilder b = new StringBuilder();
            while (c >= 0 && ",}] \n\r\t".indexOf(c) < 0) {
               b.append((char) c);
               c = in.read();
            }
            if (c >= 0) {
               in.unread(c);
            }
            String t = b.toString();
            return switch (t) {
               case "null" -> null;
               case "true" -> Boolean.TRUE;
               case "false" -> Boolean.FALSE;
               default -> Long.parseLong(t);
            };
         }
      }
   }

   static String string(PushbackReader in) throws IOException {
      StringBuilder b = new StringBuilder();
      for (int c = in.read(); c != '"'; c = in.read()) {
         if (c == '\\') {
            c = in.read();
            switch (c) {
               case 'n' -> b.append('\n');
               case 't' -> b.append('\t');
               case 'u' -> {
                  char[] hex = new char[4];
                  in.read(hex);
                  b.append((char) Integer.parseInt(new String(hex), 16));
               }
               default -> b.append((char) c);
            }
         } else {
            b.append((char) c);
         }
      }
      return b.toString();
   }
}
