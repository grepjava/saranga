// Shared helper for the packet fixture generators in this directory.
// Encodes packets with the real Artemis classes and writes one file per case:
//
//   <out>/<RustStruct>[~case]@<s|c>-<version>.bin
//
// s = decoded by the server (client to server), c = decoded by the client.
// The file holds the complete frame exactly as Artemis encodes it, with
// channel ID 7. Run a generator with scripts/packet-fixtures.sh <Group>.

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;

public final class Fixtures {
   private final Path out;
   private int count;

   public Fixtures(String[] args) throws Exception {
      if (args.length != 1) {
         throw new IllegalArgumentException("usage: <Generator>.java <out-dir>");
      }
      out = Paths.get(args[0]);
      Files.createDirectories(out);
   }

   /** Writes one fixture. {@code dir} is "s" or "c". */
   public void emit(String name, String dir, int version, Packet packet) throws Exception {
      if (!dir.equals("s") && !dir.equals("c")) {
         throw new IllegalArgumentException("dir must be s or c: " + dir);
      }
      packet.setChannelID(7);
      ActiveMQBuffer buffer = packet.encode(connection(version));
      byte[] bytes = new byte[buffer.writerIndex()];
      buffer.getBytes(0, bytes);
      Files.write(out.resolve(name + "@" + dir + "-" + version + ".bin"), bytes);
      count++;
   }

   public void done() {
      System.out.println(count + " fixtures -> " + out);
   }

   /** A deterministic message with a body, every header set, and properties. */
   public static CoreMessage message(long id) {
      CoreMessage m = new CoreMessage(id, 64);
      m.setAddress("fixture.address");
      m.setType((byte) 3);
      m.setDurable(true);
      m.setExpiration(1_800_000_000_000L);
      m.setTimestamp(1_700_000_000_000L);
      m.setPriority((byte) 7);
      m.setUserID(new org.apache.activemq.artemis.utils.UUID(
         org.apache.activemq.artemis.utils.UUID.TYPE_TIME_BASED, new byte[] {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}));
      m.putStringProperty("colour", "red");
      m.putIntProperty("n", 42);
      m.getBodyBuffer().writeNullableSimpleString(
         org.apache.activemq.artemis.api.core.SimpleString.of("fixture body"));
      return m;
   }

   /** A connection stub that reports {@code version} and allocates buffers. */
   public static CoreRemotingConnection connection(int version) {
      InvocationHandler handler = (proxy, method, args) -> {
         switch (method.getName()) {
            case "getChannelVersion":
               return version;
            case "createTransportBuffer":
               return ActiveMQBuffers.dynamicBuffer((Integer) args[0]);
            case "toString":
               return "FixtureConnection(" + version + ")";
            default:
               if (method.isDefault()) {
                  return InvocationHandler.invokeDefault(proxy, method, args);
               }
               throw new UnsupportedOperationException(method.getName());
         }
      };
      return (CoreRemotingConnection) Proxy.newProxyInstance(
         Fixtures.class.getClassLoader(), new Class<?>[] {CoreRemotingConnection.class}, handler);
   }
}
