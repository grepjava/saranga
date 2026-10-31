// The interceptors suite's client: for each protocol, what the broker's
// incoming and outgoing interceptors (Interceptors.java, and Saranga's
// under the same names) do to it. An incoming interceptor refuses what
// carries "refuse" and marks the rest <protocol>_in=seen; an outgoing one
// refuses a delivery carrying "hide" and marks the rest
// <protocol>_out=seen. MQTT's go by topic.
//
//   java -cp '<artemis>/lib/*' Suite.java host:port [test-name...]
//
// Run by tests/interop/interceptors/interceptors.sh [saranga|artemis].

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
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
      tests.put("core", Suite::core);
      tests.put("stomp", Suite::stomp);
      tests.put("mqtt", Suite::mqtt);
      tests.put("openwire", Suite::openwire);
      tests.put("amqp", Suite::amqp);
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

   // ------------------------------------------------------------------ Core

   static void core() throws Exception {
      var cf = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(
         "tcp://" + host + ":" + port + "?blockOnNonDurableSend=false");
      try (Connection c = cf.createConnection()) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue("ic.core");
         MessageProducer p = s.createProducer(q);
         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         MessageConsumer consumer = s.createConsumer(q);
         TextMessage refused = s.createTextMessage("refused");
         refused.setStringProperty("refuse", "1");
         p.send(refused);
         p.send(s.createTextMessage("ok"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("ok"), "ok received: " + got);
         check("seen".equals(got.getStringProperty("core_in")), "core_in");
         check("seen".equals(got.getStringProperty("core_out")), "core_out");
         check(consumer.receive(500) == null, "the refused send went no further");
         TextMessage hidden = s.createTextMessage("hidden");
         hidden.setStringProperty("hide", "1");
         p.send(hidden);
         check(consumer.receive(1500) == null, "the hidden delivery is not received");
      }
   }

   // ----------------------------------------------------------------- STOMP

   static String stompFrame(String command, String... headers) {
      StringBuilder b = new StringBuilder(command).append('\n');
      String body = "";
      for (String h : headers) {
         if (h.startsWith("body=")) {
            body = h.substring(5);
         } else {
            b.append(h).append('\n');
         }
      }
      return b.append('\n').append(body).append('\0').toString();
   }

   /** The frames read until nothing comes for {@code quietMillis}. */
   static List<String> stompRead(Socket socket, int quietMillis) throws IOException {
      List<String> frames = new ArrayList<>();
      socket.setSoTimeout(quietMillis);
      InputStream in = socket.getInputStream();
      ByteArrayOutputStream cur = new ByteArrayOutputStream();
      try {
         int b;
         while ((b = in.read()) != -1) {
            if (b == 0) {
               frames.add(cur.toString(StandardCharsets.UTF_8).replaceFirst("^\n+", ""));
               cur.reset();
            } else {
               cur.write(b);
            }
         }
      } catch (SocketTimeoutException e) {
         // quiet
      }
      return frames;
   }

   static void stompSend(Socket socket, String frame) throws IOException {
      socket.getOutputStream().write(frame.getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();
   }

   static boolean has(List<String> frames, String start, String line) {
      return frames.stream().anyMatch(f -> f.startsWith(start) && Arrays.asList(f.split("\n")).contains(line));
   }

   static void stomp() throws Exception {
      try (Socket socket = new Socket(host, port)) {
         stompSend(socket, stompFrame("CONNECT", "accept-version:1.2", "host:localhost"));
         check(stompRead(socket, 2000).stream().anyMatch(f -> f.startsWith("CONNECTED")), "CONNECTED");
         stompSend(socket, stompFrame("SUBSCRIBE", "destination:ic.stomp", "subscription-type:ANYCAST", "id:0", "receipt:s"));
         check(has(stompRead(socket, 1000), "RECEIPT", "receipt-id:s"), "subscribed");
         stompSend(socket, stompFrame("SEND", "destination:ic.stomp", "destination-type:ANYCAST", "refuse:1", "receipt:r1", "body=refused"));
         List<String> after = stompRead(socket, 1500);
         check(after.isEmpty(), "nothing for the refused SEND: " + after);
         stompSend(socket, stompFrame("SEND", "destination:ic.stomp", "destination-type:ANYCAST", "receipt:r2", "body=ok"));
         List<String> frames = stompRead(socket, 1500);
         check(has(frames, "RECEIPT", "receipt-id:r2"), "receipt for the SEND: " + frames);
         check(has(frames, "MESSAGE", "stomp_in:seen"), "stomp_in: " + frames);
         check(has(frames, "RECEIPT", "stomp_out:seen"), "stomp_out on the RECEIPT: " + frames);
         // A MESSAGE is encoded before the interceptors see it
         // (StompSession.sendMessage's getEncodedSize).
         check(!has(frames, "MESSAGE", "stomp_out:seen"), "no stomp_out on the MESSAGE: " + frames);
         check(frames.stream().noneMatch(f -> f.startsWith("MESSAGE") && f.endsWith("refused")), "the refused SEND went no further");
         stompSend(socket, stompFrame("SEND", "destination:ic.stomp", "destination-type:ANYCAST", "hide:1", "receipt:r3", "body=hidden"));
         frames = stompRead(socket, 1500);
         check(has(frames, "RECEIPT", "receipt-id:r3"), "receipt for the hidden SEND: " + frames);
         check(frames.stream().noneMatch(f -> f.startsWith("MESSAGE")), "the hidden MESSAGE is not sent: " + frames);
      }
   }

   // ------------------------------------------------------------------ MQTT

   static void mqttString(DataOutputStream out, String s) throws IOException {
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      out.writeShort(b.length);
      out.write(b);
   }

   static byte[] mqttPacket(int byte1, byte[] body) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(byte1);
      int len = body.length;
      do {
         int digit = len % 128;
         len /= 128;
         out.write(len > 0 ? digit | 0x80 : digit);
      } while (len > 0);
      out.writeBytes(body);
      return out.toByteArray();
   }

   static byte[] mqttPublish(String topic, String payload) throws IOException {
      ByteArrayOutputStream body = new ByteArrayOutputStream();
      DataOutputStream d = new DataOutputStream(body);
      mqttString(d, topic);
      d.write(payload.getBytes(StandardCharsets.UTF_8));
      return mqttPacket(0x30, body.toByteArray());
   }

   /** The next packet's type and body; null on timeout, type -1 at the end. */
   static Object[] mqttRead(Socket socket, int timeout) throws IOException {
      socket.setSoTimeout(timeout);
      InputStream in = socket.getInputStream();
      int b1;
      try {
         b1 = in.read();
      } catch (SocketTimeoutException e) {
         return null;
      } catch (SocketException e) {
         return new Object[] {-1, new byte[0]};
      }
      if (b1 == -1) {
         return new Object[] {-1, new byte[0]};
      }
      int len = 0, mul = 1, digit;
      do {
         digit = in.read();
         len += (digit & 127) * mul;
         mul *= 128;
      } while ((digit & 128) != 0);
      byte[] body = in.readNBytes(len);
      return new Object[] {b1 >> 4, body};
   }

   static String publishTopic(byte[] body) {
      int n = ((body[0] & 0xff) << 8) | (body[1] & 0xff);
      return new String(body, 2, n, StandardCharsets.UTF_8);
   }

   static void mqtt() throws Exception {
      try (Socket socket = new Socket(host, port)) {
         OutputStream out = socket.getOutputStream();
         ByteArrayOutputStream body = new ByteArrayOutputStream();
         DataOutputStream d = new DataOutputStream(body);
         mqttString(d, "MQTT");
         d.writeByte(4);
         d.writeByte(0x02);
         d.writeShort(60);
         mqttString(d, "ic-mqtt");
         out.write(mqttPacket(0x10, body.toByteArray()));
         Object[] p = mqttRead(socket, 3000);
         check(p != null && (int) p[0] == 2, "CONNACK");
         body.reset();
         d.writeShort(1);
         mqttString(d, "hide");
         d.writeByte(0);
         mqttString(d, "ok");
         d.writeByte(0);
         out.write(mqttPacket(0x82, body.toByteArray()));
         p = mqttRead(socket, 3000);
         check(p != null && (int) p[0] == 9, "SUBACK");
         out.write(mqttPublish("hide", "h"));
         out.write(mqttPublish("ok", "o"));
         List<String> topics = new ArrayList<>();
         while ((p = mqttRead(socket, 1500)) != null && (int) p[0] != -1) {
            if ((int) p[0] == 3) {
               topics.add(publishTopic((byte[]) p[1]));
            }
         }
         check(topics.equals(List.of("ok")), "only ok is delivered: " + topics);
         out.write(mqttPublish("refuse", "r"));
         p = mqttRead(socket, 5000);
         check(p != null && (int) p[0] == -1, "the connection closes on the refused PUBLISH");
      }
   }

   // -------------------------------------------------------------- OpenWire

   static void openwire() throws Exception {
      var cf = new org.apache.activemq.ActiveMQConnectionFactory("tcp://" + host + ":" + port);
      try (Connection c = cf.createConnection()) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue("ic.ow");
         MessageProducer p = s.createProducer(q);
         MessageConsumer consumer = s.createConsumer(q);
         p.send(s.createTextMessage("ok"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("ok"), "ok received: " + got);
         check("seen".equals(got.getStringProperty("ow_in")), "ow_in");
         check("seen".equals(got.getStringProperty("ow_out")), "ow_out");
         TextMessage hidden = s.createTextMessage("hidden");
         hidden.setStringProperty("hide", "1");
         p.send(hidden);
         check(consumer.receive(1500) == null, "the hidden dispatch is not received");
      }
      Connection c = cf.createConnection();
      try {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(s.createQueue("ic.ow.refused"));
         TextMessage refused = s.createTextMessage("refused");
         refused.setStringProperty("refuse", "1");
         try {
            p.send(refused);
            throw new AssertionError("the refused send was answered");
         } catch (JMSException expected) {
            // The broker closed the connection.
         }
      } finally {
         try {
            c.close();
         } catch (JMSException closed) {
            // Its transport is gone.
         }
      }
   }

   // ------------------------------------------------------------------ AMQP

   static void amqp() throws Exception {
      var cf = new org.apache.qpid.jms.JmsConnectionFactory("amqp://" + host + ":" + port);
      try (Connection c = cf.createConnection()) {
         c.start();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue q = s.createQueue("ic.amqp");
         MessageProducer p = s.createProducer(q);
         MessageConsumer consumer = s.createConsumer(q);
         TextMessage refused = s.createTextMessage("refused");
         refused.setStringProperty("refuse", "1");
         try {
            p.send(refused);
            throw new AssertionError("the refused send was accepted");
         } catch (JMSException expected) {
            check(String.valueOf(expected.getMessage()).contains("Interceptor rejected message"),
                  "rejected by the interceptor: " + expected.getMessage());
         }
         p.send(s.createTextMessage("ok"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("ok"), "ok received: " + got);
         // AMQPMessage.putStringProperty changes the message but does not
         // mark it modified: its encoding goes out as it came.
         check(got.getStringProperty("amqp_in") == null, "no amqp_in on the wire");
         check(got.getStringProperty("amqp_out") == null, "no amqp_out on the wire");
         TextMessage hidden = s.createTextMessage("hidden");
         hidden.setStringProperty("hide", "1");
         p.send(hidden);
         check(consumer.receive(1500) == null, "the hidden delivery is not received");
      }
      // A Core consumer gets the message as the incoming interceptor
      // changed it.
      try (Connection c = cf.createConnection();
           Connection core = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(
              "tcp://" + host + ":" + port).createConnection()) {
         core.start();
         Session cs = core.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = cs.createConsumer(cs.createQueue("ic.amqp.core"));
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         s.createProducer(s.createQueue("ic.amqp.core")).send(s.createTextMessage("to core"));
         TextMessage got = (TextMessage) consumer.receive(5000);
         check(got != null && got.getText().equals("to core"), "received over Core: " + got);
         check("seen".equals(got.getStringProperty("amqp_in")), "amqp_in over Core");
      }
   }
}
