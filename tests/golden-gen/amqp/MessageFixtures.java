// AMQP message fixtures from Artemis 2.57.0's own classes, one
// "<case> <kind> <hex>" line each:
//
//   input    an AMQP message encoded by Proton-J
//   persist  AMQPMessagePersisterV3 after setMessageID(100), setAddress("addr")
//   dla      a copy moved to a dead-letter address (QueueImpl.makeCopy), persisted
//   divert   a copy routed by a divert (DivertImpl), persisted
//   prop     after putStringProperty("added", "x") and reencode, persisted
//   send2    getSendBuffer(2): what a redelivery sends
//   core     toCore(), written by CoreMessagePersister
//   core-in  a Core message (CoreMessagePersister) ...
//   amqp     ... and CoreAmqpConverter.fromCore of it (its send buffer)
//
//   java -cp '<artemis lib>/*' tests/golden-gen/amqp/MessageFixtures.java tests/golden/amqp/messages.txt

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersisterV3;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.CoreAmqpConverter;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;

public final class MessageFixtures {
   static final DecoderImpl DECODER = new DecoderImpl();
   static final EncoderImpl ENCODER = new EncoderImpl(DECODER);
   static {
      AMQPDefinedTypes.registerAllTypes(DECODER, ENCODER);
   }
   static final StringBuilder OUT = new StringBuilder();

   static String hex(byte[] b) {
      StringBuilder s = new StringBuilder();
      for (byte x : b) {
         s.append(String.format("%02x", x & 0xff));
      }
      return s.toString();
   }

   static void emit(String name, String kind, byte[] bytes) {
      OUT.append(name).append(' ').append(kind).append(' ').append(hex(bytes)).append('\n');
   }

   static byte[] sections(Section... sections) {
      ByteBuffer buf = ByteBuffer.allocate(1 << 16);
      ENCODER.setByteBuffer(buf);
      for (Section s : sections) {
         if (s != null) {
            ENCODER.writeObject(s);
         }
      }
      buf.flip();
      byte[] out = new byte[buf.remaining()];
      buf.get(out);
      return out;
   }

   static byte[] persist(Message m) {
      ActiveMQBuffer buf = ActiveMQBuffers.dynamicBuffer(1024);
      if (m instanceof AMQPMessage) {
         AMQPMessagePersisterV3.getInstance().encode(buf, m);
      } else {
         CoreMessagePersister.getInstance().encode(buf, m);
      }
      byte[] out = new byte[buf.writerIndex()];
      buf.getBytes(0, out);
      return out;
   }

   static byte[] readable(ReadableBuffer b) {
      byte[] out = new byte[b.remaining()];
      b.get(out);
      return out;
   }

   static AMQPStandardMessage message(byte[] data) {
      AMQPStandardMessage m = new AMQPStandardMessage(0, data, null);
      m.setMessageID(100);
      m.setAddress("addr");
      return m;
   }

   static void amqpCase(String name, Section... sections) throws Exception {
      byte[] data = sections(sections);
      emit(name, "input", data);
      emit(name, "persist", persist(message(data)));

      AMQPStandardMessage original = message(data);
      Message dla = original.copy(200);
      dla.setAddress("DLA");
      dla.referenceOriginalMessage(original, SimpleString.of("q"));
      dla.setExpiration(0);
      dla.setRoutingType(null);
      dla.setBrokerProperty(Message.HDR_ACTUAL_EXPIRY_TIME, 1234L);
      dla.reencode();
      emit(name, "dla", persist(dla));

      original = message(data);
      Message divert = original.copy(300);
      divert.referenceOriginalMessage(original, SimpleString.of("div"));
      divert.setAddress("other");
      divert.setExpiration(original.getExpiration());
      divert.setRoutingType(RoutingType.MULTICAST);
      divert.reencode();
      emit(name, "divert", persist(divert));

      AMQPStandardMessage prop = message(data);
      prop.putStringProperty("added", "x");
      prop.reencode();
      emit(name, "prop", persist(prop));

      emit(name, "send2", readable(message(data).getSendBuffer(2, null)));
      emit(name, "core", persist(message(data).toCore()));
   }

   static void coreCase(String name, CoreMessage m) throws Exception {
      emit(name, "core-in", persist(m));
      AMQPMessage amqp = CoreAmqpConverter.fromCore(m, null);
      emit(name, "amqp", readable(amqp.getSendBuffer(1, null)));
   }

   static CoreMessage core(byte type) {
      CoreMessage m = new CoreMessage(500, 512);
      m.setType(type);
      m.setAddress("core.addr");
      m.setUserID(new org.apache.activemq.artemis.utils.UUID(
         org.apache.activemq.artemis.utils.UUID.TYPE_TIME_BASED,
         new byte[] {1, 2, 3, 4, 5, 6, 0x11, 8, 9, 10, 11, 12, 13, 14, 15, 16}));
      return m;
   }

   static Symbol sym(String s) {
      return Symbol.valueOf(s);
   }

   public static void main(String[] args) throws Exception {
      Header durable = new Header();
      durable.setDurable(true);
      durable.setPriority(UnsignedByte.valueOf((byte) 7));
      Map<Symbol, Object> ma = new LinkedHashMap<>();
      ma.put(sym("x-opt-jms-msg-type"), (byte) 5);
      ma.put(sym("x-opt-jms-dest"), (byte) 0);
      Properties props = new Properties();
      props.setMessageId("ID:abc:1:1:1-1");
      props.setTo("q1");
      props.setSubject("type");
      props.setReplyTo("reply");
      props.setCorrelationId("corr");
      props.setCreationTime(new Date(1_700_000_000_000L));
      props.setGroupId("g");
      props.setGroupSequence(UnsignedInteger.valueOf(3));
      props.setUserId(new Binary("bob".getBytes(StandardCharsets.UTF_8)));
      Map<String, Object> app = new LinkedHashMap<>();
      app.put("s", "v");
      app.put("i", 1);
      app.put("l", 2L);
      app.put("b", true);
      app.put("d", 1.5d);
      app.put("f", 2.5f);
      app.put("sh", (short) 3);
      app.put("by", (byte) 4);
      app.put("bin", new Binary(new byte[] {1, 2}));
      app.put("ui", UnsignedInteger.valueOf(7));
      app.put("ul", UnsignedLong.valueOf(8));
      app.put("sym", sym("sy"));
      app.put("n", null);
      app.put("_AMQ_DUPL_ID", "dup");
      Map<Symbol, Object> footer = new LinkedHashMap<>();
      footer.put(sym("x-opt-f"), 1);
      amqpCase("text", durable, new MessageAnnotations(ma), props, new ApplicationProperties(app),
               new AmqpValue("hello"), new Footer(footer));

      Properties textPlain = new Properties();
      textPlain.setContentType(sym("text/plain;charset=utf-8"));
      amqpCase("data-text", textPlain, new Data(new Binary("héllo".getBytes(StandardCharsets.UTF_8))));
      amqpCase("data-bytes", new Data(new Binary(new byte[] {1, 2, 3})));
      Properties octet = new Properties();
      octet.setContentType(sym("application/octet-stream"));
      amqpCase("data-octet", octet, new Data(new Binary(new byte[] {4, 5})));
      Properties object = new Properties();
      object.setContentType(sym("application/x-java-serialized-object"));
      amqpCase("object", object, new Data(new Binary(new byte[] {(byte) 0xac, (byte) 0xed, 0, 5})));
      amqpCase("value-binary", new AmqpValue(new Binary(new byte[] {7, 8})));
      Map<String, Object> mapBody = new LinkedHashMap<>();
      mapBody.put("a", 1);
      mapBody.put("b", "x");
      mapBody.put("c", new Binary(new byte[] {9}));
      mapBody.put("d", UnsignedInteger.valueOf(5));
      mapBody.put("e", true);
      amqpCase("value-map", new AmqpValue(mapBody));
      List<Object> list = new ArrayList<>(List.of(1, "x", true, 2L));
      amqpCase("value-list", new AmqpValue(list));
      amqpCase("sequence", new AmqpSequence(new ArrayList<>(List.of(1, "y"))));
      Header nonDurable = new Header();
      nonDurable.setDurable(false);
      amqpCase("no-body", nonDurable);
      Properties textOnly = new Properties();
      textOnly.setContentType(sym("text/plain"));
      amqpCase("no-body-text", textOnly);
      amqpCase("value-int", new AmqpValue(42));
      Properties uuid = new Properties();
      uuid.setMessageId(new UUID(1, 2));
      uuid.setCorrelationId(new Binary(new byte[] {5, 6}));
      amqpCase("ids-uuid", uuid, new AmqpValue("u"));
      Properties ulong = new Properties();
      ulong.setMessageId(UnsignedLong.valueOf(5));
      ulong.setCorrelationId(new UUID(3, 4));
      amqpCase("ids-ulong", ulong, new AmqpValue("u"));
      Properties binary = new Properties();
      binary.setMessageId(new Binary(new byte[] {0x0a, (byte) 0xbc}));
      binary.setCorrelationId("ID:AMQP_ULONG:9");
      amqpCase("ids-binary", binary, new AmqpValue("u"));
      Properties plain = new Properties();
      plain.setMessageId("plain");
      plain.setCorrelationId(UnsignedLong.valueOf(77));
      amqpCase("ids-plain", plain, new AmqpValue("u"));
      Header counted = new Header();
      counted.setDeliveryCount(UnsignedInteger.valueOf(0));
      Map<Symbol, Object> da = new LinkedHashMap<>();
      da.put(sym("x-opt-d"), "v");
      amqpCase("delivery-annotations", counted, new DeliveryAnnotations(da),
               new MessageAnnotations(new LinkedHashMap<>(Map.of(sym("x-opt-m"), 1))), new AmqpValue("x"));
      amqpCase("delivery-annotations-only", new DeliveryAnnotations(da), new AmqpValue("x"));
      Map<Symbol, Object> routing = new LinkedHashMap<>();
      routing.put(sym("x-opt-routing-type"), (byte) 1);
      Properties expiry = new Properties();
      expiry.setAbsoluteExpiryTime(new Date(1_800_000_000_000L));
      amqpCase("routing", new MessageAnnotations(routing), expiry, new AmqpValue("r"));
      Map<Symbol, Object> scheduled = new LinkedHashMap<>();
      scheduled.put(sym("x-opt-delivery-time"), 1_900_000_000_000L);
      amqpCase("scheduled", new MessageAnnotations(scheduled), new AmqpValue("s"));
      Map<Symbol, Object> topics = new LinkedHashMap<>();
      topics.put(sym("x-opt-jms-dest"), (byte) 1);
      topics.put(sym("x-opt-jms-reply-to"), (byte) 3);
      Properties topicProps = new Properties();
      topicProps.setTo("t");
      topicProps.setReplyTo("tt");
      amqpCase("topics", new MessageAnnotations(topics), topicProps, new AmqpValue("t"));
      Header first = new Header();
      first.setDeliveryCount(UnsignedInteger.valueOf(3));
      first.setFirstAcquirer(true);
      amqpCase("header-counts", first, new AmqpValue("h"));
      Map<String, Object> appOnly = new LinkedHashMap<>();
      appOnly.put("k", "v");
      amqpCase("app-only", new ApplicationProperties(appOnly), new Data(new Binary(new byte[] {1})));

      // Core to AMQP.
      CoreMessage text = core(Message.TEXT_TYPE);
      text.getBodyBuffer().writeNullableSimpleString(SimpleString.of("hello core"));
      text.setDurable(true);
      text.setPriority((byte) 7);
      text.setTimestamp(1_700_000_000_000L);
      text.setRoutingType(RoutingType.ANYCAST);
      text.putStringProperty("s", "v");
      text.putIntProperty("i", 1);
      text.putLongProperty("l", 2L);
      text.putBooleanProperty("b", true);
      text.putBytesProperty("bytes", new byte[] {1, 2});
      text.putShortProperty("sh", (short) 3);
      text.putByteProperty("by", (byte) 4);
      text.putDoubleProperty("d", 1.5);
      text.putFloatProperty("f", 2.5f);
      text.putCharProperty("c", 'z');
      text.putStringProperty("JMSType", "jt");
      text.putStringProperty("JMSCorrelationID", "corr");
      text.putStringProperty("JMSReplyTo", "topic://replies");
      text.putStringProperty("_AMQ_GROUP_ID", "grp");
      text.putIntProperty("_AMQ_GROUP_SEQUENCE", 4);
      text.putStringProperty("_AMQ_VALIDATED_USER", "alice");
      text.putLongProperty("_AMQ_SCHED_DELIVERY", 1_900_000_000_000L);
      coreCase("core-text", text);
      CoreMessage bytes = core(Message.BYTES_TYPE);
      bytes.getBodyBuffer().writeBytes(new byte[] {1, 2, 3});
      bytes.putBytesProperty("JMSCorrelationID", new byte[] {7});
      coreCase("core-bytes", bytes);
      CoreMessage map = core(Message.MAP_TYPE);
      TypedProperties body = new TypedProperties();
      body.putIntProperty(SimpleString.of("a"), 1);
      body.putSimpleStringProperty(SimpleString.of("b"), SimpleString.of("x"));
      body.putBytesProperty(SimpleString.of("c"), new byte[] {9});
      body.putBooleanProperty(SimpleString.of("e"), true);
      body.encode(map.getBodyBuffer().byteBuf());
      coreCase("core-map", map);
      CoreMessage stream = core(Message.STREAM_TYPE);
      stream.getBodyBuffer().writeByte((byte) 6);
      stream.getBodyBuffer().writeInt(1);
      stream.getBodyBuffer().writeByte((byte) 10);
      stream.getBodyBuffer().writeNullableString("x");
      stream.getBodyBuffer().writeByte((byte) 2);
      stream.getBodyBuffer().writeBoolean(true);
      // A byte[] item fails here: Proton-J cannot encode a byte[] in the
      // AmqpValue list that Artemis builds.
      coreCase("core-stream", stream);
      CoreMessage obj = core(Message.OBJECT_TYPE);
      obj.getBodyBuffer().writeInt(3);
      obj.getBodyBuffer().writeBytes(new byte[] {(byte) 0xac, (byte) 0xed, 0});
      coreCase("core-object", obj);
      CoreMessage empty = core(Message.DEFAULT_TYPE);
      empty.setRoutingType(RoutingType.MULTICAST);
      coreCase("core-default", empty);
      CoreMessage nullText = core(Message.TEXT_TYPE);
      nullText.getBodyBuffer().writeNullableSimpleString(null);
      coreCase("core-null-text", nullText);

      // AMQP to Core and back: the JMS_AMQP_ properties restore the
      // original sections.
      for (String name : new String[] {"text", "data-text", "value-binary", "sequence", "no-body", "topics"}) {
         String in = OUT.toString().lines().filter(l -> l.startsWith(name + " input ")).findFirst().get();
         byte[] data = new byte[(in.length() - name.length() - 7) / 2];
         String h = in.substring(name.length() + 7);
         for (int i = 0; i < data.length; i++) {
            data[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
         }
         ICoreMessage converted = message(data).toCore();
         coreCase("rt-" + name, (CoreMessage) converted);
      }

      Files.writeString(Paths.get(args[0]), OUT.toString());
      System.out.println(OUT.toString().split("\n").length + " fixtures -> " + args[0]);
   }
}
