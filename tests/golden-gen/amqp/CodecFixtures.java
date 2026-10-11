// AMQP codec fixtures: values encoded by Proton-J 0.35.0's EncoderImpl, the
// encoder Artemis uses, one "<name> <hex>" line each. Saranga must decode
// each one and re-encode it to the same bytes.
//
//   java -cp '<artemis lib>/*' tests/golden-gen/amqp/CodecFixtures.java tests/golden/amqp/codec.txt

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Received;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.security.SaslChallenge;
import org.apache.qpid.proton.amqp.security.SaslCode;
import org.apache.qpid.proton.amqp.security.SaslInit;
import org.apache.qpid.proton.amqp.security.SaslMechanisms;
import org.apache.qpid.proton.amqp.security.SaslOutcome;
import org.apache.qpid.proton.amqp.security.SaslResponse;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.End;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Role;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

public final class CodecFixtures {
   static final DecoderImpl DECODER = new DecoderImpl();
   static final EncoderImpl ENCODER = new EncoderImpl(DECODER);
   static {
      AMQPDefinedTypes.registerAllTypes(DECODER, ENCODER);
   }
   static final StringBuilder OUT = new StringBuilder();

   static void emit(String name, Object value) {
      ByteBuffer buf = ByteBuffer.allocate(1 << 20);
      ENCODER.setByteBuffer(buf);
      ENCODER.writeObject(value);
      buf.flip();
      StringBuilder hex = new StringBuilder();
      while (buf.hasRemaining()) {
         hex.append(String.format("%02x", buf.get() & 0xff));
      }
      OUT.append(name).append(' ').append(hex).append('\n');
   }

   static String repeat(char c, int n) {
      char[] a = new char[n];
      Arrays.fill(a, c);
      return new String(a);
   }

   static Symbol sym(String s) {
      return Symbol.valueOf(s);
   }

   public static void main(String[] args) throws Exception {
      // Primitives and their boundaries.
      emit("null", null);
      emit("true", true);
      emit("false", false);
      emit("ubyte", UnsignedByte.valueOf((byte) 200));
      emit("ushort", UnsignedShort.valueOf((short) 60000));
      for (long v : new long[] {0, 1, 255, 256, 0xffffffffL}) {
         emit("uint-" + v, UnsignedInteger.valueOf(v));
      }
      for (long v : new long[] {0, 1, 255, 256, Long.MAX_VALUE}) {
         emit("ulong-" + v, UnsignedLong.valueOf(v));
      }
      emit("byte", (byte) -5);
      emit("short", (short) -300);
      for (int v : new int[] {0, -128, 127, 128, -129, Integer.MIN_VALUE}) {
         emit("int-" + v, v);
      }
      for (long v : new long[] {0, -128, 127, 128, -129, Long.MIN_VALUE}) {
         emit("long-" + v, v);
      }
      emit("float", 1.5f);
      emit("double", -2.25d);
      emit("decimal32", new Decimal32(0x01020304));
      emit("decimal64", new Decimal64(0x0102030405060708L));
      emit("decimal128", new Decimal128(0x0102030405060708L, 0x090a0b0c0d0e0f10L));
      emit("char", 'x');
      emit("timestamp", new Date(1_700_000_000_123L));
      emit("uuid", new UUID(0x0123456789abcdefL, 0xfedcba9876543210L));
      for (int n : new int[] {0, 255, 256}) {
         emit("string-" + n, repeat('s', n));
         emit("symbol-" + n, sym(repeat('y', n)));
         emit("binary-" + n, new Binary(new byte[n]));
      }
      emit("string-utf8", "héllo € 😀");

      // Lists around the list8/list32 boundary: n elements of str8 "x"*k.
      emit("list-empty", new ArrayList<>());
      for (int size : new int[] {250, 251, 252, 253, 254, 255, 256}) {
         // one string of (size - 2) chars encodes to `size` bytes
         emit("list-content-" + size, new ArrayList<>(List.of(repeat('l', size - 2))));
      }
      List<Object> many = new ArrayList<>();
      for (int i = 0; i < 256; i++) {
         many.add(null);
      }
      emit("list-256-nulls", many);
      emit("list-mixed", new ArrayList<>(Arrays.asList(1, "two", sym("three"), null, true, 4L)));

      // Maps around the map8/map32 boundary.
      emit("map-empty", new LinkedHashMap<>());
      for (int size : new int[] {251, 252, 253, 254, 255}) {
         Map<Object, Object> m = new LinkedHashMap<>();
         m.put(sym("k"), repeat('m', size - 5));
         emit("map-content-" + size, m);
      }
      for (int n : new int[] {127, 128}) {
         Map<Object, Object> m = new LinkedHashMap<>();
         for (int i = 0; i < n; i++) {
            m.put(i, null);
         }
         emit("map-entries-" + n, m);
      }
      Map<Object, Object> nested = new LinkedHashMap<>();
      nested.put("list", new ArrayList<>(List.of(1, 2)));
      nested.put("map", new LinkedHashMap<>(Map.of("a", 1L)));
      nested.put(UnsignedLong.valueOf(5), new Binary(new byte[] {1, 2}));
      emit("map-nested", nested);

      // Arrays.
      emit("array-symbol", new Symbol[] {sym("a"), sym("bc")});
      emit("array-symbol-long", new Symbol[] {sym("a"), sym(repeat('z', 300))});
      emit("array-symbol-empty", new Symbol[0]);
      emit("array-int", new int[] {1, -1, 100000});
      emit("array-long", new long[] {1, -1});
      emit("array-boolean", new boolean[] {true, false});
      emit("array-string", new String[] {"x", "y"});
      emit("array-uint", new UnsignedInteger[] {UnsignedInteger.valueOf(1), UnsignedInteger.valueOf(70000)});
      emit("array-ulong", new UnsignedLong[] {UnsignedLong.valueOf(1)});
      emit("array-double", new double[] {1.0, 2.0});
      Symbol[] big = new Symbol[100];
      Arrays.fill(big, sym("abc"));
      emit("array-symbol-100", big);

      // Transport performatives.
      Open open = new Open();
      open.setContainerId("container-1");
      emit("open-minimal", open);
      open.setHostname("localhost");
      open.setMaxFrameSize(UnsignedInteger.valueOf(131072));
      open.setChannelMax(UnsignedShort.valueOf((short) 65535));
      open.setIdleTimeOut(UnsignedInteger.valueOf(30000));
      open.setOutgoingLocales(sym("en-US"));
      open.setIncomingLocales(sym("en-US"), sym("fr"));
      open.setOfferedCapabilities(sym("ANONYMOUS-RELAY"), sym("SHARED-SUBS"));
      open.setDesiredCapabilities(sym("DELAYED_DELIVERY"));
      Map<Symbol, Object> props = new LinkedHashMap<>();
      props.put(sym("product"), "apache-activemq-artemis");
      props.put(sym("version"), "2.57.0");
      open.setProperties(props);
      emit("open-full", open);

      Begin begin = new Begin();
      begin.setNextOutgoingId(UnsignedInteger.valueOf(1));
      begin.setIncomingWindow(UnsignedInteger.valueOf(2147483647));
      begin.setOutgoingWindow(UnsignedInteger.valueOf(2147483647));
      emit("begin", begin);
      begin.setRemoteChannel(UnsignedShort.valueOf((short) 3));
      begin.setHandleMax(UnsignedInteger.valueOf(65535));
      emit("begin-reply", begin);

      Source source = new Source();
      source.setAddress("queue://q1");
      source.setDurable(TerminusDurability.UNSETTLED_STATE);
      source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
      source.setTimeout(UnsignedInteger.valueOf(0));
      source.setDynamic(false);
      source.setDistributionMode(sym("copy"));
      Map<Symbol, Object> filters = new LinkedHashMap<>();
      filters.put(sym("jms-selector"), new SelectorFilter("color = 'red'"));
      source.setFilter(filters);
      source.setDefaultOutcome(new Modified());
      source.setOutcomes(sym("amqp:accepted:list"), sym("amqp:rejected:list"));
      source.setCapabilities(sym("topic"), sym("shared"));
      Target target = new Target();
      target.setAddress("topic://t1");
      target.setCapabilities(sym("queue"));
      Attach attach = new Attach();
      attach.setName("link-1");
      attach.setHandle(UnsignedInteger.valueOf(0));
      attach.setRole(Role.RECEIVER);
      attach.setSndSettleMode(SenderSettleMode.UNSETTLED);
      attach.setRcvSettleMode(ReceiverSettleMode.FIRST);
      attach.setSource(source);
      attach.setTarget(target);
      emit("attach-receiver", attach);
      Attach sender = new Attach();
      sender.setName("sender");
      sender.setHandle(UnsignedInteger.valueOf(1));
      sender.setRole(Role.SENDER);
      sender.setSndSettleMode(SenderSettleMode.SETTLED);
      sender.setSource(new Source());
      sender.setTarget(new Target());
      sender.setInitialDeliveryCount(UnsignedInteger.valueOf(0));
      sender.setMaxMessageSize(UnsignedLong.valueOf(1024));
      sender.setOfferedCapabilities(sym("x"));
      Map<Symbol, Object> lp = new LinkedHashMap<>();
      lp.put(sym("paired"), true);
      sender.setProperties(lp);
      emit("attach-sender", sender);
      Attach coord = new Attach();
      coord.setName("txn");
      coord.setHandle(UnsignedInteger.valueOf(2));
      coord.setRole(Role.SENDER);
      coord.setSource(new Source());
      Coordinator coordinator = new Coordinator();
      coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
      coord.setTarget(coordinator);
      emit("attach-coordinator", coord);
      Source deleteOnClose = new Source();
      deleteOnClose.setDynamic(true);
      deleteOnClose.setDynamicNodeProperties(Collections.singletonMap(sym("lifetime-policy"), DeleteOnClose.getInstance()));
      emit("source-dynamic", deleteOnClose);

      Flow flow = new Flow();
      flow.setNextIncomingId(UnsignedInteger.valueOf(1));
      flow.setIncomingWindow(UnsignedInteger.valueOf(2047));
      flow.setNextOutgoingId(UnsignedInteger.valueOf(1));
      flow.setOutgoingWindow(UnsignedInteger.valueOf(2147483647));
      emit("flow-session", flow);
      flow.setHandle(UnsignedInteger.valueOf(0));
      flow.setDeliveryCount(UnsignedInteger.valueOf(5));
      flow.setLinkCredit(UnsignedInteger.valueOf(1000));
      flow.setAvailable(UnsignedInteger.valueOf(0));
      flow.setDrain(true);
      flow.setEcho(false);
      emit("flow-link", flow);

      Transfer transfer = new Transfer();
      transfer.setHandle(UnsignedInteger.valueOf(0));
      transfer.setDeliveryId(UnsignedInteger.valueOf(0));
      transfer.setDeliveryTag(new Binary(new byte[] {0}));
      transfer.setMessageFormat(UnsignedInteger.valueOf(0));
      emit("transfer-first", transfer);
      transfer.setSettled(true);
      transfer.setMore(true);
      transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);
      TransactionalState ts = new TransactionalState();
      ts.setTxnId(new Binary(new byte[] {0, 0, 0, 1}));
      ts.setOutcome(Accepted.getInstance());
      transfer.setState(ts);
      transfer.setResume(false);
      transfer.setAborted(false);
      transfer.setBatchable(true);
      emit("transfer-full", transfer);
      Transfer cont = new Transfer();
      cont.setHandle(UnsignedInteger.valueOf(0));
      cont.setMore(false);
      emit("transfer-continuation", cont);

      Disposition disposition = new Disposition();
      disposition.setRole(Role.RECEIVER);
      disposition.setFirst(UnsignedInteger.valueOf(3));
      disposition.setSettled(true);
      disposition.setState(Accepted.getInstance());
      emit("disposition-accepted", disposition);
      disposition.setLast(UnsignedInteger.valueOf(9));
      disposition.setBatchable(false);
      Rejected rejected = new Rejected();
      ErrorCondition err = new ErrorCondition(sym("amqp:not-found"), "no such queue");
      err.setInfo(Collections.singletonMap(sym("k"), "v"));
      rejected.setError(err);
      disposition.setState(rejected);
      emit("disposition-rejected", disposition);
      Modified modified = new Modified();
      modified.setDeliveryFailed(true);
      modified.setUndeliverableHere(false);
      modified.setMessageAnnotations(Collections.singletonMap(sym("x-opt-a"), 1));
      emit("modified", modified);
      emit("released", Released.getInstance());
      emit("accepted", Accepted.getInstance());
      Received received = new Received();
      received.setSectionNumber(UnsignedInteger.valueOf(2));
      received.setSectionOffset(UnsignedLong.valueOf(100));
      emit("received", received);

      Detach detach = new Detach();
      detach.setHandle(UnsignedInteger.valueOf(4));
      detach.setClosed(true);
      emit("detach", detach);
      detach.setError(new ErrorCondition(sym("amqp:unauthorized-access"), "denied"));
      emit("detach-error", detach);
      emit("end", new End());
      emit("close", new Close());
      Close close = new Close();
      close.setError(new ErrorCondition(sym("amqp:connection:forced"), null));
      emit("close-error", close);

      // SASL.
      SaslMechanisms mechs = new SaslMechanisms();
      mechs.setSaslServerMechanisms(sym("PLAIN"), sym("ANONYMOUS"));
      emit("sasl-mechanisms", mechs);
      SaslMechanisms one = new SaslMechanisms();
      one.setSaslServerMechanisms(sym("PLAIN"));
      emit("sasl-mechanisms-one", one);
      SaslInit init = new SaslInit();
      init.setMechanism(sym("PLAIN"));
      init.setInitialResponse(new Binary("\0user\0pass".getBytes()));
      init.setHostname("localhost");
      emit("sasl-init", init);
      SaslChallenge challenge = new SaslChallenge();
      challenge.setChallenge(new Binary(new byte[] {1, 2}));
      emit("sasl-challenge", challenge);
      SaslResponse response = new SaslResponse();
      response.setResponse(new Binary(new byte[] {3}));
      emit("sasl-response", response);
      SaslOutcome outcome = new SaslOutcome();
      outcome.setCode(SaslCode.OK);
      emit("sasl-outcome-ok", outcome);
      outcome.setCode(SaslCode.AUTH);
      outcome.setAdditionalData(new Binary(new byte[] {9}));
      emit("sasl-outcome-auth", outcome);

      // Transactions.
      emit("declare", new Declare());
      Discharge discharge = new Discharge();
      discharge.setTxnId(new Binary(new byte[] {0, 0, 0, 7}));
      discharge.setFail(true);
      emit("discharge", discharge);
      Declared declared = new Declared();
      declared.setTxnId(new Binary(new byte[] {0, 0, 0, 7}));
      emit("declared", declared);
      emit("txn-state", ts);

      // Message sections.
      Header header = new Header();
      emit("header-empty", header);
      header.setDurable(true);
      emit("header-durable", header);
      header.setPriority(UnsignedByte.valueOf((byte) 7));
      header.setTtl(UnsignedInteger.valueOf(60000));
      header.setFirstAcquirer(false);
      header.setDeliveryCount(UnsignedInteger.valueOf(2));
      emit("header-full", header);
      Header dc = new Header();
      dc.setDeliveryCount(UnsignedInteger.valueOf(1));
      emit("header-delivery-count", dc);

      Properties p = new Properties();
      p.setMessageId("ID:abc");
      emit("properties-id", p);
      p.setUserId(new Binary("user".getBytes()));
      p.setTo("queue://q");
      p.setSubject("subject");
      p.setReplyTo("queue://reply");
      p.setCorrelationId(UnsignedLong.valueOf(42));
      p.setContentType(sym("text/plain"));
      p.setContentEncoding(sym("utf-8"));
      p.setAbsoluteExpiryTime(new Date(1_800_000_000_000L));
      p.setCreationTime(new Date(1_700_000_000_000L));
      p.setGroupId("group");
      p.setGroupSequence(UnsignedInteger.valueOf(3));
      p.setReplyToGroupId("rgroup");
      emit("properties-full", p);
      Properties uuidId = new Properties();
      uuidId.setMessageId(new UUID(1, 2));
      uuidId.setCorrelationId(new Binary(new byte[] {5}));
      emit("properties-uuid", uuidId);

      Map<String, Object> app = new LinkedHashMap<>();
      app.put("s", "v");
      app.put("i", 1);
      app.put("l", 2L);
      app.put("b", true);
      app.put("d", 1.0d);
      app.put("f", 2.0f);
      app.put("sh", (short) 3);
      app.put("by", (byte) 4);
      app.put("c", 'c');
      app.put("n", null);
      app.put("bin", new Binary(new byte[] {1}));
      emit("application-properties", new ApplicationProperties(app));
      Map<Symbol, Object> ma = new LinkedHashMap<>();
      ma.put(sym("x-opt-jms-msg-type"), (byte) 5);
      ma.put(sym("x-opt-jms-dest"), (byte) 0);
      ma.put(sym("x-opt-delivery-time"), 1_700_000_000_000L);
      emit("message-annotations", new MessageAnnotations(ma));
      emit("delivery-annotations", new DeliveryAnnotations(Collections.singletonMap(sym("x-opt-d"), "v")));
      emit("data", new Data(new Binary("hello".getBytes())));
      emit("data-empty", new Data(new Binary(new byte[0])));
      emit("data-large", new Data(new Binary(new byte[1000])));
      emit("amqp-value-string", new AmqpValue("text body"));
      emit("amqp-value-null", new AmqpValue(null));
      Map<String, Object> mapBody = new LinkedHashMap<>();
      mapBody.put("a", 1);
      emit("amqp-value-map", new AmqpValue(mapBody));
      emit("amqp-value-binary", new AmqpValue(new Binary(new byte[] {1, 2, 3})));
      emit("amqp-sequence", new AmqpSequence(new ArrayList<>(List.of(1, "x"))));
      emit("footer", new Footer(Collections.singletonMap(sym("x-opt-f"), 1)));

      // Where Proton-J's fast-path types pick list8 or list32.
      emit("properties-empty", new Properties());
      for (int n : new int[] {200, 201}) {
         Transfer t = new Transfer();
         t.setHandle(UnsignedInteger.valueOf(0));
         t.setDeliveryTag(new Binary(new byte[n]));
         emit("transfer-tag-" + n, t);
      }
      Transfer t150 = new Transfer();
      t150.setHandle(UnsignedInteger.valueOf(0));
      t150.setDeliveryTag(new Binary(new byte[150]));
      t150.setMessageFormat(UnsignedInteger.valueOf(70000));
      t150.setDeliveryId(UnsignedInteger.valueOf(70000));
      t150.setRcvSettleMode(ReceiverSettleMode.FIRST);
      emit("transfer-150", t150);
      Transfer released = new Transfer();
      released.setHandle(UnsignedInteger.valueOf(0));
      released.setState(Released.getInstance());
      emit("transfer-released", released);
      for (Object state : new Object[] {Released.getInstance(), modified, received, null}) {
         Disposition d = new Disposition();
         d.setRole(Role.SENDER);
         d.setFirst(UnsignedInteger.valueOf(1));
         d.setState((org.apache.qpid.proton.amqp.transport.DeliveryState) state);
         emit("disposition-" + (state == null ? "none" : state.getClass().getSimpleName()), d);
      }
      Disposition fresh = new Disposition();
      fresh.setRole(Role.RECEIVER);
      fresh.setFirst(UnsignedInteger.valueOf(1));
      fresh.setState(new Accepted());
      emit("disposition-new-accepted", fresh);
      Flow pflow = new Flow();
      pflow.setIncomingWindow(UnsignedInteger.valueOf(1));
      pflow.setNextOutgoingId(UnsignedInteger.valueOf(1));
      pflow.setOutgoingWindow(UnsignedInteger.valueOf(1));
      pflow.setProperties(Collections.singletonMap(sym("p"), 1));
      emit("flow-properties", pflow);
      Begin pbegin = new Begin();
      pbegin.setNextOutgoingId(UnsignedInteger.valueOf(1));
      pbegin.setIncomingWindow(UnsignedInteger.valueOf(1));
      pbegin.setOutgoingWindow(UnsignedInteger.valueOf(1));
      pbegin.setOfferedCapabilities(sym("a"));
      pbegin.setProperties(Collections.singletonMap(sym("p"), 1));
      emit("begin-properties", pbegin);
      Open bigOpen = new Open();
      bigOpen.setContainerId(repeat('c', 300));
      emit("open-big", bigOpen);
      Open boundaryOpen = new Open();
      boundaryOpen.setContainerId(repeat('c', 251));
      emit("open-253", boundaryOpen);
      boundaryOpen.setContainerId(repeat('c', 252));
      emit("open-254", boundaryOpen);
      Detach bigDetach = new Detach();
      bigDetach.setHandle(UnsignedInteger.valueOf(1));
      bigDetach.setError(new ErrorCondition(sym("amqp:internal-error"), repeat('e', 300)));
      emit("detach-big", bigDetach);
      Attach bigAttach = new Attach();
      bigAttach.setName(repeat('n', 300));
      bigAttach.setHandle(UnsignedInteger.valueOf(1));
      bigAttach.setRole(Role.SENDER);
      emit("attach-big", bigAttach);
      Header hd = new Header();
      hd.setDurable(false);
      hd.setFirstAcquirer(true);
      emit("header-false", hd);
      Map<String, Object> bigApp = new LinkedHashMap<>();
      bigApp.put("k", repeat('v', 300));
      emit("application-properties-big", new ApplicationProperties(bigApp));
      emit("application-properties-empty", new ApplicationProperties(new LinkedHashMap<>()));
      emit("message-annotations-empty", new MessageAnnotations(new LinkedHashMap<>()));
      Source defaults = new Source();
      defaults.setAddress("a");
      defaults.setDurable(TerminusDurability.NONE);
      defaults.setExpiryPolicy(TerminusExpiryPolicy.SESSION_END);
      defaults.setTimeout(UnsignedInteger.ZERO);
      defaults.setDynamic(false);
      emit("source-defaults", defaults);
      Target tdefaults = new Target();
      tdefaults.setAddress("a");
      tdefaults.setDurable(TerminusDurability.CONFIGURATION);
      emit("target-durable", tdefaults);
      for (int n : new int[] {248, 249}) {
         emit("array-content-" + (n + 5), new Symbol[] {sym(repeat('a', n))});
      }
      emit("data-253", new Data(new Binary(new byte[253])));
      emit("data-254", new Data(new Binary(new byte[254])));

      Files.writeString(Paths.get(args[0]), OUT.toString());
      System.out.println(OUT.toString().split("\n").length + " fixtures -> " + args[0]);
   }

   /** The apache.org:selector-filter:string described type Qpid JMS sends. */
   static final class SelectorFilter implements DescribedType {
      final String selector;

      SelectorFilter(String selector) {
         this.selector = selector;
      }

      @Override
      public Object getDescriptor() {
         return UnsignedLong.valueOf(0x0000468C00000004L);
      }

      @Override
      public Object getDescribed() {
         return selector;
      }
   }
}
