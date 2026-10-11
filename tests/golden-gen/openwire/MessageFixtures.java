/*
 * Message conversion fixtures from Artemis 2.57.0's OpenWireMessageConverter,
 * one "<case> <kind> <hex>" line each:
 *
 *   input     an OpenWire message, marshalled (version 12, tight, no cache)
 *   core      inbound() of it, written by CoreMessagePersister
 *   error     inbound() failed: the exception's class and message, in UTF-8
 *   core-in   a Core message (CoreMessagePersister) ...
 *   params    ... the delivery count and the consumer's destination ...
 *   dispatch  ... and createMessageDispatch of it, marshalled as above
 *             (node id 11223344-5566-11ee-8001-020304050607, sequence 55)
 *
 * Every case converted inbound is also delivered back ("rt-<case>"). Each
 * conversion gets its own fresh OpenWireFormat, as a new connection would.
 *
 *   java -cp '<artemis>/lib/*' MessageFixtures.java <out file>
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;

public final class MessageFixtures {

   static final StringBuilder OUT = new StringBuilder();
   static final UUID NODE = new UUID(UUID.TYPE_TIME_BASED, new byte[] {
      0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x11, (byte) 0xee, (byte) 0x80, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07});

   public static void main(String[] args) throws Exception {
      inboundCases();
      outboundCases();
      Files.writeString(Path.of(args[0]), OUT.toString());
   }

   static OpenWireFormat format() {
      OpenWireFormat wf = new OpenWireFormat(12);
      wf.setTightEncodingEnabled(true);
      wf.setCacheEnabled(false);
      return wf;
   }

   static String hex(byte[] b) {
      StringBuilder sb = new StringBuilder();
      for (byte x : b) {
         sb.append(String.format("%02x", x & 0xff));
      }
      return sb.toString();
   }

   static void emit(String name, String kind, byte[] bytes) {
      OUT.append(name).append(' ').append(kind).append(' ').append(hex(bytes)).append('\n');
   }

   static byte[] bytes(ByteSequence s) {
      byte[] b = new byte[s.getLength()];
      System.arraycopy(s.getData(), s.getOffset(), b, 0, s.getLength());
      return b;
   }

   static byte[] persist(ICoreMessage m) {
      ActiveMQBuffer buf = ActiveMQBuffers.dynamicBuffer(1024);
      CoreMessagePersister.getInstance().encode(buf, m);
      byte[] out = new byte[buf.writerIndex()];
      buf.getBytes(0, out);
      return out;
   }

   // ------------------------------------------------------------ inbound

   static ActiveMQMessage base(ActiveMQMessage m) throws Exception {
      ProducerId producer = new ProducerId(new SessionId(new ConnectionId("ID:client-1-1700000000000-0:1"), 1), 1);
      m.setProducerId(producer);
      m.setMessageId(new MessageId(producer, 7));
      m.setDestination(new ActiveMQQueue("orders"));
      m.setTimestamp(1700000000123L);
      m.setPersistent(true);
      m.setPriority((byte) 4);
      m.setCommandId(9);
      return m;
   }

   static void inbound(String name, ActiveMQMessage m) throws Exception {
      OpenWireFormat wire = format();
      // As the client sends it: beforeMarshall stores the body.
      ByteSequence frame = wire.marshal(m);
      emit(name, "input", bytes(frame));
      // What the broker reads.
      ActiveMQMessage received = (ActiveMQMessage) format().unmarshal(frame);
      ICoreMessage core;
      try {
         core = (ICoreMessage) OpenWireMessageConverter.inbound(received, format(), null);
      } catch (Throwable e) {
         emit(name, "error", (e.getClass().getName() + ": " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
         return;
      }
      emit(name, "core", persist(core));
      ((CoreMessage) core).setMessageID(1234);
      core.setAddress("orders");
      outbound("rt-" + name, (CoreMessage) core, 1, new ActiveMQQueue("orders"));
   }

   static byte[] deflate(byte[] data) throws Exception {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      try (DeflaterOutputStream os = new DeflaterOutputStream(bytesOut)) {
         os.write(data);
      }
      return bytesOut.toByteArray();
   }

   static byte[] utf8(String s) throws Exception {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      DataOutputStream d = new DataOutputStream(b);
      MarshallingSupport.writeUTF8(d, s);
      d.close();
      return b.toByteArray();
   }

   static void inboundCases() throws Exception {
      ActiveMQTextMessage text = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      text.setText("hello é 😀 \u0000 end");
      text.setStringProperty("s", "value");
      text.setIntProperty("i", 42);
      text.setLongProperty("l", -5L);
      text.setBooleanProperty("b", true);
      text.setDoubleProperty("d", 1.5);
      text.setFloatProperty("f", 2.25f);
      text.setShortProperty("sh", (short) 3);
      text.setByteProperty("by", (byte) -2);
      text.setObjectProperty("c", 'x');
      text.setStringProperty("nul", "a\u0000b 😀");
      text.setProperty("bytes", new byte[] {1, 2, 3});
      Map<String, Object> nested = new HashMap<>();
      nested.put("k", 1);
      nested.put("s", "v");
      text.setProperty("map", nested);
      List<Object> list = new ArrayList<>();
      list.add("a");
      list.add(2L);
      list.add(1.0);
      text.setProperty("list", list);
      text.setJMSCorrelationID("corr-1");
      text.setJMSType("my-type");
      text.setGroupID("group-a");
      text.setGroupSequence(3);
      text.setUserID("alice");
      text.setDroppable(true);
      text.setExpiration(1800000000000L);
      text.setArrival(1700000000200L);
      text.setBrokerInTime(1700000000300L);
      text.setBrokerPath(new BrokerId[] {new BrokerId("b1"), new BrokerId("b2")});
      text.setCluster(new BrokerId[] {new BrokerId("c1")});
      text.setOriginalDestination(new ActiveMQTopic("orig"));
      text.setReplyTo(new ActiveMQTempQueue("ID:client-1-1700000000000-0:1:1"));
      inbound("text-full", text);

      for (ActiveMQDestination reply : new ActiveMQDestination[] {
         new ActiveMQQueue("rq"), new ActiveMQTopic("rt"), new ActiveMQTempTopic("ID:x:1:2")}) {
         ActiveMQTextMessage t = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
         t.setText("r");
         t.setReplyTo(reply);
         inbound("reply-" + reply.getDestinationTypeAsString(), t);
      }

      ActiveMQTextMessage nullText = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      inbound("text-null", nullText);

      ActiveMQTextMessage compressedText = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      compressedText.setContent(new ByteSequence(deflate(utf8("squeezed text"))));
      compressedText.setCompressed(true);
      inbound("text-compressed", compressedText);

      ActiveMQBytesMessage bytes = (ActiveMQBytesMessage) base(new ActiveMQBytesMessage());
      bytes.writeBytes(new byte[] {9, 8, 7, 6});
      bytes.writeInt(77);
      inbound("bytes", bytes);

      ActiveMQBytesMessage emptyBytes = (ActiveMQBytesMessage) base(new ActiveMQBytesMessage());
      inbound("bytes-none", emptyBytes);

      ActiveMQBytesMessage compressedBytes = (ActiveMQBytesMessage) base(new ActiveMQBytesMessage());
      byte[] raw = "compressed bytes body".getBytes(StandardCharsets.US_ASCII);
      byte[] packed = deflate(raw);
      byte[] withLength = new byte[4 + packed.length];
      withLength[0] = 0;
      withLength[1] = 0;
      withLength[2] = 0;
      withLength[3] = (byte) raw.length;
      System.arraycopy(packed, 0, withLength, 4, packed.length);
      compressedBytes.setContent(new ByteSequence(withLength));
      compressedBytes.setCompressed(true);
      inbound("bytes-compressed", compressedBytes);

      ActiveMQMapMessage map = (ActiveMQMapMessage) base(new ActiveMQMapMessage());
      map.setString("name", "widget");
      map.setInt("count", 3);
      map.setLong("big", 1L << 40);
      map.setBoolean("flag", false);
      map.setDouble("price", 9.99);
      map.setFloat("ratio", 0.5f);
      map.setShort("sh", (short) -1);
      map.setByte("by", (byte) 1);
      map.setChar("ch", 'z');
      map.setBytes("raw", new byte[] {4, 5});
      map.setObject("nothing", null);
      inbound("map", map);

      ActiveMQMapMessage emptyMap = (ActiveMQMapMessage) base(new ActiveMQMapMessage());
      inbound("map-none", emptyMap);

      ActiveMQMapMessage nestedMap = (ActiveMQMapMessage) base(new ActiveMQMapMessage());
      nestedMap.setObject("inner", nested);
      inbound("map-nested", nestedMap);

      ActiveMQObjectMessage object = (ActiveMQObjectMessage) base(new ActiveMQObjectMessage());
      object.setObject("a serialized string");
      inbound("object", object);

      ActiveMQStreamMessage stream = (ActiveMQStreamMessage) base(new ActiveMQStreamMessage());
      stream.writeBoolean(true);
      stream.writeByte((byte) 5);
      stream.writeBytes(new byte[] {1, 2});
      stream.writeChar('q');
      stream.writeDouble(2.5);
      stream.writeFloat(1.25f);
      stream.writeInt(-9);
      stream.writeLong(123456789012L);
      stream.writeShort((short) 300);
      stream.writeString("str");
      stream.writeString(null);
      stream.writeString("x".repeat(9000));
      inbound("stream", stream);

      ActiveMQMessage plain = base(new ActiveMQMessage());
      plain.setStringProperty("only", "props");
      inbound("plain", plain);

      ActiveMQMessage plainBody = base(new ActiveMQMessage());
      plainBody.setContent(new ByteSequence(new byte[] {1, 2, 3, 4}));
      inbound("plain-content", plainBody);

      ActiveMQTextMessage scheduled = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      scheduled.setText("later");
      scheduled.setLongProperty("AMQ_SCHEDULED_DELAY", 60000L);
      inbound("scheduled", scheduled);

      ActiveMQTextMessage withDs = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      withDs.setText("advisory-ish");
      ConsumerInfo info = new ConsumerInfo(new ConsumerId(new SessionId(new ConnectionId("ID:c"), 1), 2));
      info.setDestination(new ActiveMQQueue("watched"));
      info.setPrefetchSize(1000);
      withDs.setDataStructure(info);
      inbound("datastructure", withDs);

      ActiveMQTextMessage many = (ActiveMQTextMessage) base(new ActiveMQTextMessage());
      many.setText("many");
      for (int i = 0; i < 40; i++) {
         many.setIntProperty("prop" + i, i);
      }
      inbound("many-properties", many);

      ActiveMQTextMessage noIds = new ActiveMQTextMessage();
      noIds.setDestination(new ActiveMQQueue("orders"));
      noIds.setText("bare");
      inbound("bare", noIds);
   }

   // ------------------------------------------------------------ outbound

   static AMQConsumer consumer(ActiveMQDestination dest) throws Exception {
      Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
      AMQConsumer c = (AMQConsumer) unsafe.allocateInstance(AMQConsumer.class);
      ConsumerInfo info = new ConsumerInfo(new ConsumerId(new SessionId(new ConnectionId("ID:consumer-conn"), 3), 4));
      info.setDestination(dest);
      set(c, "info", info);
      set(c, "openwireDestination", dest);
      return c;
   }

   static void set(Object o, String field, Object value) throws Exception {
      Field f = o.getClass().getDeclaredField(field);
      f.setAccessible(true);
      f.set(o, value);
   }

   static void outbound(String name, CoreMessage core, int deliveryCount, ActiveMQDestination dest) throws Exception {
      emit(name, "core-in", persist(core));
      emit(name, "params", (deliveryCount + " " + dest.getQualifiedName()).getBytes(StandardCharsets.UTF_8));
      MessageReference ref = (MessageReference) Proxy.newProxyInstance(
         MessageFixtures.class.getClassLoader(), new Class<?>[] {MessageReference.class},
         (proxy, method, args) -> switch (method.getName()) {
            case "getDeliveryCount" -> deliveryCount;
            default -> throw new UnsupportedOperationException(method.getName());
         });
      MessageDispatch md;
      try {
         md = OpenWireMessageConverter.createMessageDispatch(ref, core, format(), consumer(dest), NODE, 55);
      } catch (Throwable e) {
         emit(name, "error", (e.getClass().getName() + ": " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
         return;
      }
      emit(name, "dispatch", bytes(format().marshal(md)));
   }

   static CoreMessage core(byte type) {
      CoreMessage m = new CoreMessage(3000000123L, 512);
      m.setType(type);
      m.setAddress("orders");
      m.setDurable(true);
      m.setTimestamp(1700000000000L);
      m.setPriority((byte) 6);
      m.setExpiration(0);
      return m;
   }

   static void outboundCases() throws Exception {
      CoreMessage text = core(org.apache.activemq.artemis.api.core.Message.TEXT_TYPE);
      text.getBodyBuffer().writeNullableSimpleString(SimpleString.of("from core é 😀"));
      text.putStringProperty("s", "v");
      text.putIntProperty("i", 1);
      text.putLongProperty("l", 2L);
      text.putBooleanProperty("b", true);
      text.putDoubleProperty("d", 0.25);
      text.putFloatProperty("f", 4.5f);
      text.putShortProperty("sh", (short) 7);
      text.putByteProperty("by", (byte) 8);
      text.putCharProperty("ch", 'c');
      text.putBytesProperty("raw", new byte[] {(byte) 0xab, 0x01});
      text.putObjectProperty("nul", null);
      text.putStringProperty("JMSType", "core-type");
      text.putStringProperty("JMSCorrelationID", "core-corr");
      text.putStringProperty("_AMQ_GROUP_ID", "g1");
      text.putIntProperty("_AMQ_GROUP_SEQUENCE", 5);
      text.putStringProperty("JMSReplyTo", "topic://replies");
      text.putLongProperty("JMSXDeliveryCount", 3L);
      text.putStringProperty("_AMQ_LVQ_NAME", "last");
      text.putLongProperty("_AMQ_INGRESS_TIMESTAMP", 1700000000999L);
      text.putStringProperty("__HDR_dlqDeliveryFailureCause", "poisoned");
      text.putStringProperty("__HDR_USER_ID", "bob");
      text.putBooleanProperty("__HDR_DROPPABLE", true);
      text.putStringProperty("__HDR_BROKER_PATH", "x,y,");
      text.putStringProperty("__HDR_ORIG_DESTINATION", "topic://was-here");
      text.putStringProperty("_AMQ_DUPL_ID", "dup");
      outbound("core-text", text, 2, new ActiveMQQueue("orders"));

      CoreMessage setters = core(org.apache.activemq.artemis.api.core.Message.TEXT_TYPE);
      setters.getBodyBuffer().writeNullableSimpleString(null);
      setters.putIntProperty("JMSPriority", 9);
      setters.putStringProperty("JMSDeliveryMode", "NON_PERSISTENT");
      setters.putBooleanProperty("JMSRedelivered", true);
      setters.putLongProperty("JMSTimestamp", 42L);
      setters.putLongProperty("JMSExpiration", 43L);
      setters.putIntProperty("JMSXGroupSeq", 11);
      setters.putStringProperty("JMSXGroupID", "gx");
      setters.putIntProperty("JMSXDeliveryCount", 6);
      setters.putLongProperty("JMSPriorityLong", 1L);
      setters.putFloatProperty("JMSExpirationF", 1f);
      outbound("core-setters", setters, 1, new ActiveMQTopic("orders"));

      CoreMessage elsewhere = core(org.apache.activemq.artemis.api.core.Message.BYTES_TYPE);
      elsewhere.getBodyBuffer().writeBytes(new byte[] {1, 2, 3});
      elsewhere.setAddress("diverted");
      outbound("core-bytes-other-address", elsewhere, 1, new ActiveMQTempQueue("ID:t:1:1"));
      outbound("core-bytes-topic", elsewhere, 4, new ActiveMQTopic("orders"));

      CoreMessage map = core(org.apache.activemq.artemis.api.core.Message.MAP_TYPE);
      TypedProperties body = new TypedProperties();
      body.putSimpleStringProperty(SimpleString.of("name"), SimpleString.of("widget"));
      body.putIntProperty(SimpleString.of("count"), 3);
      body.putBytesProperty(SimpleString.of("raw"), new byte[] {6});
      body.putCharProperty(SimpleString.of("ch"), 'k');
      body.putNullValue(SimpleString.of("none"));
      for (int i = 0; i < 20; i++) {
         body.putLongProperty(SimpleString.of("k" + i), i);
      }
      body.encode(map.getBodyBuffer().byteBuf());
      outbound("core-map", map, 1, new ActiveMQQueue("orders"));

      CoreMessage nullMap = core(org.apache.activemq.artemis.api.core.Message.MAP_TYPE);
      new TypedProperties().encode(nullMap.getBodyBuffer().byteBuf());
      outbound("core-map-null", nullMap, 1, new ActiveMQQueue("orders"));

      CoreMessage noMap = core(org.apache.activemq.artemis.api.core.Message.MAP_TYPE);
      outbound("core-map-empty-body", noMap, 1, new ActiveMQQueue("orders"));

      CoreMessage stream = core(org.apache.activemq.artemis.api.core.Message.STREAM_TYPE);
      ActiveMQBuffer sb = stream.getBodyBuffer();
      sb.writeByte((byte) 2);
      sb.writeBoolean(true);
      sb.writeByte((byte) 3);
      sb.writeByte((byte) 9);
      sb.writeByte((byte) 4);
      sb.writeInt(2);
      sb.writeBytes(new byte[] {7, 7});
      sb.writeByte((byte) 11);
      sb.writeShort((short) 'w');
      sb.writeByte((byte) 9);
      sb.writeLong(Double.doubleToLongBits(3.5));
      sb.writeByte((byte) 8);
      sb.writeInt(Float.floatToIntBits(0.75f));
      sb.writeByte((byte) 6);
      sb.writeInt(100);
      sb.writeByte((byte) 7);
      sb.writeLong(-100L);
      sb.writeByte((byte) 5);
      sb.writeShort((short) -3);
      sb.writeByte((byte) 10);
      sb.writeNullableString("stream text");
      sb.writeByte((byte) 10);
      sb.writeNullableString(null);
      sb.writeByte((byte) 10);
      sb.writeNullableString("y".repeat(8200));
      sb.writeByte((byte) 99);
      sb.writeByte((byte) 6);
      sb.writeInt(1);
      outbound("core-stream", stream, 1, new ActiveMQQueue("orders"));

      CoreMessage object = core(org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE);
      object.getBodyBuffer().writeInt(3);
      object.getBodyBuffer().writeBytes(new byte[] {(byte) 0xac, (byte) 0xed, 0});
      outbound("core-object", object, 1, new ActiveMQQueue("orders"));

      CoreMessage emptyObject = core(org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE);
      outbound("core-object-empty", emptyObject, 1, new ActiveMQQueue("orders"));

      CoreMessage plain = core(org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE);
      outbound("core-default", plain, 1, new ActiveMQQueue("orders"));

      CoreMessage notification = core(org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE);
      notification.putStringProperty("_AMQ_NotifType", "CONSUMER_CREATED");
      notification.putStringProperty("_AMQ_Address", "orders");
      notification.putIntProperty("_AMQ_ConsumerCount", 1);
      notification.putStringProperty("__HDR_MESSAGE_ID", "ID:notif-1:2:3:4");
      outbound("core-notification", notification, 1, new ActiveMQTopic("activemq.notifications"));

      CoreMessage many = core(org.apache.activemq.artemis.api.core.Message.TEXT_TYPE);
      many.getBodyBuffer().writeNullableSimpleString(SimpleString.of("many"));
      for (int i = 0; i < 30; i++) {
         many.putStringProperty("p" + i, "v" + i);
      }
      outbound("core-many-properties", many, 1, new ActiveMQQueue("orders"));

      CoreMessage small = core(org.apache.activemq.artemis.api.core.Message.TEXT_TYPE);
      small.setMessageID(17);
      small.getBodyBuffer().writeNullableSimpleString(SimpleString.of("small id"));
      outbound("core-small-id", small, 1, new ActiveMQQueue("orders"));

      CoreMessage compressed = core(org.apache.activemq.artemis.api.core.Message.TEXT_TYPE);
      compressed.getBodyBuffer().writeNullableSimpleString(SimpleString.of("compress me"));
      compressed.putBooleanProperty("__HDR_COMPRESSED", true);
      outbound("core-text-compressed", compressed, 1, new ActiveMQQueue("orders"));
   }
}
