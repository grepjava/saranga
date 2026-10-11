/*
 * Golden fixtures for the OpenWire codec, made with the ActiveMQ 5.19.10
 * marshallers that Artemis 2.57.0 ships.
 *
 * For each version (1 to 12) and four combinations of tight encoding, cache
 * and stack traces, it builds random data structures of every type the
 * version knows, marshals them in sequence with one OpenWireFormat (so the
 * cache carries over), and writes each frame in hex with a dump of what it
 * holds:
 *
 *   config <tight> <cache> <stack>
 *   frame <hex>
 *   dump <Class{Field=value,...}>
 *
 * The fields of a dump are those fields.txt (from generate.py) lists for the
 * class and version, in order. It also checks that Java reads every frame
 * back to what was written and marshals it to the same bytes.
 *
 *   java -cp '<artemis>/lib/*' CodecFixtures.java fields.txt <out dir>
 */

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;

public class CodecFixtures {

   /** class/version -> field names. */
   static final Map<String, List<String>> FIELDS = new HashMap<>();
   /** version -> type -> class. */
   static final Map<Integer, TreeMap<Integer, String>> TYPES = new HashMap<>();

   static Random rnd;
   static int version;
   static boolean stack;

   public static void main(String[] args) throws Exception {
      for (String line : Files.readAllLines(Path.of(args[0]))) {
         if (line.startsWith("#") || line.isBlank()) {
            continue;
         }
         String[] p = line.trim().split(" ");
         int v = Integer.parseInt(p[2]);
         List<String> names = new ArrayList<>();
         for (int i = 3; i < p.length; i++) {
            names.add(p[i]);
         }
         FIELDS.put(p[0] + "/" + v, names);
         TYPES.computeIfAbsent(v, k -> new TreeMap<>()).put(Integer.parseInt(p[1]), p[0]);
      }
      Path out = Path.of(args[1]);
      Files.createDirectories(out);
      boolean[][] configs = {{true, true, true}, {true, false, false}, {false, true, false}, {false, false, true}};
      for (version = 1; version <= 12; version++) {
         StringBuilder sb = new StringBuilder();
         for (int c = 0; c < configs.length; c++) {
            boolean tight = configs[c][0], cache = configs[c][1];
            stack = configs[c][2];
            sb.append("config ").append(tight).append(' ').append(cache).append(' ').append(stack).append('\n');
            OpenWireFormat wf = format(tight, cache);
            OpenWireFormat reader = format(tight, cache);
            OpenWireFormat rewriter = format(tight, cache);
            rnd = new Random(version * 100L + c);
            for (int round = 0; round < 2; round++) {
               for (Map.Entry<Integer, String> t : TYPES.get(version).entrySet()) {
                  Object o = create(Class.forName("org.apache.activemq.command." + t.getValue()), 0);
                  ByteSequence seq = wf.marshal(o);
                  Object back = reader.unmarshal(seq);
                  // What the frame holds: v1 and v10 cut long exception
                  // messages as they write them.
                  String dump = dump(back);
                  if (!dump.equals(dump(o)) && !(version == 1 || version == 10)) {
                     System.err.println("v" + version + " " + t.getValue() + ": read back differs:\n" + dump(o) + "\n" + dump);
                  }
                  ByteSequence again = rewriter.marshal(back);
                  if (!hex(seq).equals(hex(again))) {
                     System.err.println("v" + version + " " + t.getValue() + ": remarshalled differs");
                  }
                  sb.append("frame ").append(hex(seq)).append('\n');
                  sb.append("dump ").append(dump).append('\n');
               }
            }
         }
         Files.writeString(out.resolve("v" + version + ".txt"), sb.toString());
      }
   }

   static OpenWireFormat format(boolean tight, boolean cache) {
      OpenWireFormat wf = new OpenWireFormat(version);
      wf.setTightEncodingEnabled(tight);
      wf.setCacheEnabled(cache);
      wf.setStackTraceEnabled(stack);
      return wf;
   }

   // ------------------------------------------------------------ random values

   static final String[] CONNECTIONS = {"ID:host-1-40001-1700000000000-1:1", "ID:host-2-40002-1700000000000-3:7"};
   static final String[] DESTINATIONS = {"queue.a", "TEST.FOO", "topic.ü", "VirtualTopic.Orders"};

   static String string() {
      switch (rnd.nextInt(10)) {
         case 0: return null;
         case 1: return "";
         case 2: return "héllo wörld";
         case 3: return "日本語";
         case 4: return "nul\u0000char";
         case 5: return "smile 😀";
         case 6: return "x".repeat(300 + rnd.nextInt(200));
         case 7: return "߿ࠀ\u0080\u007f";
         default: {
            StringBuilder sb = new StringBuilder();
            int n = 1 + rnd.nextInt(20);
            for (int i = 0; i < n; i++) {
               sb.append((char) (0x21 + rnd.nextInt(0x5e)));
            }
            return sb.toString();
         }
      }
   }

   static long longValue() {
      switch (rnd.nextInt(7)) {
         case 0: return 0;
         case 1: return rnd.nextInt(65536);
         case 2: return 65536L + (rnd.nextInt() & 0x7fffffffL);
         case 3: return 0xffffffffL;
         case 4: return -1;
         case 5: return 1700000000000L + rnd.nextInt(1000000);
         default: return rnd.nextLong();
      }
   }

   static int intValue() {
      switch (rnd.nextInt(4)) {
         case 0: return 0;
         case 1: return -1;
         case 2: return 1000;
         default: return rnd.nextInt();
      }
   }

   static byte[] bytes(int max) {
      byte[] b = new byte[rnd.nextInt(max + 1)];
      rnd.nextBytes(b);
      return b;
   }

   static Throwable throwable(int depth) {
      // v1 and v10 cut messages over 1024 chars, and fail on a null one.
      boolean cuts = version == 1 || version == 10;
      String msg = rnd.nextInt(4) == 0 ? (cuts ? "y".repeat(1100) : null) : string();
      if (msg == null && cuts) {
         msg = "";
      }
      Throwable t = switch (rnd.nextInt(3)) {
         case 0 -> new java.io.IOException(msg);
         case 1 -> new IllegalStateException(msg);
         default -> new javax.jms.InvalidDestinationException(msg);
      };
      StackTraceElement[] st = new StackTraceElement[rnd.nextInt(3)];
      for (int i = 0; i < st.length; i++) {
         st[i] = new StackTraceElement("org.example.C" + i, "m" + i, rnd.nextBoolean() ? null : "C" + i + ".java", rnd.nextInt(500) - 2);
      }
      t.setStackTrace(st);
      if (depth < 2 && rnd.nextBoolean()) {
         t.initCause(throwable(depth + 1));
      }
      return t;
   }

   static ConnectionId connectionId() {
      return new ConnectionId(CONNECTIONS[rnd.nextInt(CONNECTIONS.length)]);
   }

   static ActiveMQDestination destination() {
      String name = DESTINATIONS[rnd.nextInt(DESTINATIONS.length)];
      return switch (rnd.nextInt(4)) {
         case 0 -> new ActiveMQQueue(name);
         case 1 -> new ActiveMQTopic(name);
         case 2 -> new ActiveMQTempQueue(CONNECTIONS[0] + ":" + (1 + rnd.nextInt(2)));
         default -> new ActiveMQTempTopic(CONNECTIONS[1] + ":" + (1 + rnd.nextInt(2)));
      };
   }

   static boolean known(int type) {
      return TYPES.get(version).containsKey(type);
   }

   static Object create(Class<?> t, int depth) throws Exception {
      if (t == ActiveMQDestination.class) {
         return destination();
      }
      if (t == ActiveMQQueue.class || t == ActiveMQTopic.class) {
         return t.getConstructor(String.class).newInstance(DESTINATIONS[rnd.nextInt(DESTINATIONS.length)]);
      }
      if (t == ActiveMQTempQueue.class || t == ActiveMQTempTopic.class) {
         return t.getConstructor(String.class).newInstance(CONNECTIONS[rnd.nextInt(2)] + ":" + (1 + rnd.nextInt(2)));
      }
      if (t == ConnectionId.class) {
         return connectionId();
      }
      if (t == SessionId.class) {
         return new SessionId(connectionId(), 1 + rnd.nextInt(2));
      }
      if (t == ProducerId.class) {
         return new ProducerId(new SessionId(connectionId(), 1 + rnd.nextInt(2)), 1 + rnd.nextInt(2));
      }
      if (t == ConsumerId.class) {
         return new ConsumerId(new SessionId(connectionId(), 1 + rnd.nextInt(2)), 1 + rnd.nextInt(2));
      }
      if (t == BrokerId.class) {
         return new BrokerId(rnd.nextBoolean() ? "broker-1" : "ID:broker-2");
      }
      if (t == TransactionId.class) {
         if (rnd.nextBoolean()) {
            return new LocalTransactionId(connectionId(), 1 + rnd.nextInt(3));
         }
         XATransactionId xid = new XATransactionId();
         xid.setFormatId(intValue());
         xid.setGlobalTransactionId(bytes(16));
         xid.setBranchQualifier(bytes(8));
         return xid;
      }
      if (t == org.apache.activemq.command.Message.class) {
         int type = 23 + rnd.nextInt(known(29) ? 7 : 6);
         return create(Class.forName("org.apache.activemq.command." + TYPES.get(version).get(type)), depth);
      }
      if (t == DataStructure.class) {
         Class<?>[] choices = {ConsumerId.class, ProducerId.class, SessionId.class, ConnectionId.class,
            BrokerId.class, ActiveMQDestination.class, TransactionId.class, org.apache.activemq.command.ShutdownInfo.class};
         return create(choices[rnd.nextInt(choices.length)], depth);
      }
      if (t == BooleanExpression.class) {
         return create(NetworkBridgeFilter.class, depth);
      }
      Object o = t.getDeclaredConstructor().newInstance();
      List<String> fields = FIELDS.get(t.getSimpleName() + "/" + version);
      if (fields == null) {
         throw new IllegalStateException("no fields for " + t.getSimpleName() + " v" + version);
      }
      for (String f : fields) {
         Method getter = getter(t, f);
         Method setter = setter(t, f, getter.getReturnType());
         setter.invoke(o, value(f, setter.getParameterTypes()[0], depth));
      }
      return o;
   }

   static Object value(String field, Class<?> p, int depth) throws Exception {
      if (p == boolean.class) {
         return rnd.nextBoolean();
      }
      if (p == byte.class) {
         return (byte) rnd.nextInt();
      }
      if (p == short.class) {
         return (short) rnd.nextInt();
      }
      if (p == int.class) {
         return intValue();
      }
      if (p == long.class) {
         return longValue();
      }
      if (p == String.class) {
         return string();
      }
      if (p == byte[].class) {
         if (field.equals("Magic")) {
            return rnd.nextBoolean() ? "ActiveMQ".getBytes(StandardCharsets.US_ASCII) : new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
         }
         return rnd.nextInt(5) == 0 ? null : bytes(24);
      }
      if (p == ByteSequence.class) {
         if (rnd.nextInt(5) == 0) {
            return null;
         }
         byte[] b = bytes(40);
         int off = b.length == 0 ? 0 : rnd.nextInt(b.length);
         return new ByteSequence(b, off, b.length - off);
      }
      if (p == Throwable.class) {
         return rnd.nextInt(3) == 0 ? null : throwable(0);
      }
      if (p.isArray()) {
         if (depth >= 2 || rnd.nextInt(3) == 0) {
            return null;
         }
         Object a = java.lang.reflect.Array.newInstance(p.getComponentType(), rnd.nextInt(3));
         for (int i = 0; i < java.lang.reflect.Array.getLength(a); i++) {
            java.lang.reflect.Array.set(a, i, create(p.getComponentType(), depth + 1));
         }
         return a;
      }
      if (depth >= 2 || rnd.nextInt(6) == 0) {
         return null;
      }
      return create(p, depth + 1);
   }

   static Method getter(Class<?> t, String f) throws Exception {
      for (String prefix : new String[] {"get", "is"}) {
         try {
            return t.getMethod(prefix + f);
         } catch (NoSuchMethodException e) {
            // next
         }
      }
      throw new NoSuchMethodException(t.getSimpleName() + "." + f);
   }

   static Method setter(Class<?> t, String f, Class<?> type) throws Exception {
      Method any = null;
      for (Method m : t.getMethods()) {
         if (m.getName().equals("set" + f) && m.getParameterCount() == 1) {
            if (m.getParameterTypes()[0] == type) {
               return m;
            }
            any = m;
         }
      }
      if (any == null) {
         throw new NoSuchMethodException(t.getSimpleName() + ".set" + f);
      }
      return any;
   }

   // ------------------------------------------------------------ dump

   static String dump(Object o) throws Exception {
      StringBuilder sb = new StringBuilder();
      dump(sb, o);
      return sb.toString();
   }

   static void dump(StringBuilder sb, Object o) throws Exception {
      if (o == null) {
         sb.append("null");
      } else if (o instanceof DataStructure) {
         Class<?> t = o.getClass();
         sb.append(t.getSimpleName()).append('{');
         boolean first = true;
         for (String f : FIELDS.get(t.getSimpleName() + "/" + version)) {
            if (!first) {
               sb.append(',');
            }
            first = false;
            sb.append(f).append('=');
            dump(sb, getter(t, f).invoke(o));
         }
         sb.append('}');
      } else if (o instanceof String s) {
         quote(sb, s);
      } else if (o instanceof byte[] b) {
         sb.append('[').append(hex(b, 0, b.length)).append(']');
      } else if (o instanceof ByteSequence b) {
         sb.append('[').append(hex(b.getData(), b.getOffset(), b.getLength())).append(']');
      } else if (o instanceof Throwable t) {
         sb.append("Throwable{class=");
         quote(sb, t.getClass().getName());
         sb.append(",message=");
         if (t.getMessage() == null) {
            sb.append("null");
         } else {
            quote(sb, t.getMessage());
         }
         if (stack) {
            sb.append(",stack=[");
            StackTraceElement[] st = t.getStackTrace();
            for (int i = 0; i < st.length; i++) {
               if (i > 0) {
                  sb.append(',');
               }
               sb.append('(');
               quote(sb, st[i].getClassName());
               sb.append(',');
               quote(sb, st[i].getMethodName());
               sb.append(',');
               if (st[i].getFileName() == null) {
                  sb.append("null");
               } else {
                  quote(sb, st[i].getFileName());
               }
               sb.append(',').append(st[i].getLineNumber()).append(')');
            }
            sb.append("],cause=");
            dump(sb, t.getCause());
         }
         sb.append('}');
      } else if (o instanceof Object[] a) {
         sb.append('[');
         for (int i = 0; i < a.length; i++) {
            if (i > 0) {
               sb.append(',');
            }
            dump(sb, a[i]);
         }
         sb.append(']');
      } else {
         sb.append(o);
      }
   }

   static void quote(StringBuilder sb, String s) {
      sb.append('"');
      for (char c : s.toCharArray()) {
         if (c >= 0x20 && c < 0x7f && c != '"' && c != '\\') {
            sb.append(c);
         } else {
            sb.append(String.format("\\u%04x", (int) c));
         }
      }
      sb.append('"');
   }

   static String hex(ByteSequence s) {
      return hex(s.getData(), s.getOffset(), s.getLength());
   }

   static String hex(byte[] b, int off, int len) {
      StringBuilder sb = new StringBuilder();
      for (int i = off; i < off + len; i++) {
         sb.append(String.format("%02x", b[i] & 0xff));
      }
      return sb.toString();
   }
}
