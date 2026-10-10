// Fixtures for crates/saranga-core-protocol/src/packets/session.rs.

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;

public class SessionFixtures {

   /** Sets a (possibly private, inherited) field. */
   static <T> T set(T obj, String name, Object value) throws Exception {
      for (Class<?> c = obj.getClass(); c != null; c = c.getSuperclass()) {
         try {
            Field f = c.getDeclaredField(name);
            f.setAccessible(true);
            f.set(obj, value);
            return obj;
         } catch (NoSuchFieldException e) {
            // try the superclass
         }
      }
      throw new NoSuchFieldException(obj.getClass().getName() + "." + name);
   }

   static SimpleString s(String v) {
      return SimpleString.of(v);
   }

   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);
      SimpleString addr = s("orders.address");
      SimpleString queue = s("orders.queue");
      SimpleString filter = s("colour = 'red' AND n > 3");

      // ---- addresses, queues, producers
      f.emit("CreateAddressMessage", "s", 136,
         new CreateAddressMessage(addr, EnumSet.of(RoutingType.MULTICAST, RoutingType.ANYCAST), true, false));
      f.emit("CreateAddressMessage~empty", "s", 136,
         new CreateAddressMessage(s("a"), EnumSet.noneOf(RoutingType.class), false, true));
      f.emit("CreateProducerMessage", "s", 136, new CreateProducerMessage(3, addr));
      f.emit("CreateProducerMessage~null", "s", 136, new CreateProducerMessage(4, null));
      f.emit("RemoveProducerMessage", "s", 136, new RemoveProducerMessage(3));

      f.emit("CreateQueueMessage", "s", 136, new CreateQueueMessage(addr, queue, filter, true, false, true));
      f.emit("CreateQueueMessage~nullfilter", "s", 129, new CreateQueueMessage(addr, queue, null, false, true, false));

      CreateQueueMessage_V2 cq = new CreateQueueMessage_V2();
      set(cq, "address", addr);
      set(cq, "queueName", queue);
      set(cq, "filterString", filter);
      set(cq, "durable", true);
      set(cq, "temporary", false);
      set(cq, "requiresResponse", true);
      set(cq, "autoCreated", true);
      set(cq, "routingType", RoutingType.ANYCAST);
      set(cq, "maxConsumers", 10);
      set(cq, "purgeOnNoConsumers", true);
      set(cq, "exclusive", true);
      set(cq, "lastValue", false);
      set(cq, "lastValueKey", s("lvk"));
      set(cq, "nonDestructive", true);
      set(cq, "consumersBeforeDispatch", 2);
      set(cq, "delayBeforeDispatch", 5000L);
      set(cq, "groupRebalance", false);
      set(cq, "groupBuckets", 16);
      set(cq, "autoDelete", true);
      set(cq, "autoDeleteDelay", 60_000L);
      set(cq, "autoDeleteMessageCount", 0L);
      set(cq, "groupFirstKey", s("first"));
      set(cq, "ringSize", 1000L);
      set(cq, "enabled", true);
      set(cq, "groupRebalancePauseDispatch", false);
      f.emit("CreateQueueMessageV2", "s", 136, cq);
      CreateQueueMessage_V2 cqNulls = new CreateQueueMessage_V2();
      set(cqNulls, "address", addr);
      set(cqNulls, "queueName", queue);
      set(cqNulls, "maxConsumers", -1);
      f.emit("CreateQueueMessageV2~nulls", "s", 136, cqNulls);

      f.emit("CreateSharedQueueMessage", "s", 136, new CreateSharedQueueMessage(addr, queue, filter, true, true));
      f.emit("CreateSharedQueueMessage~nullfilter", "s", 129, new CreateSharedQueueMessage(addr, queue, null, false, false));

      CreateSharedQueueMessage_V2 sq = new CreateSharedQueueMessage_V2();
      set(sq, "address", addr);
      set(sq, "queueName", s("shared.sub"));
      set(sq, "filterString", filter);
      set(sq, "durable", true);
      set(sq, "routingType", RoutingType.MULTICAST);
      set(sq, "requiresResponse", true);
      set(sq, "maxConsumers", 5);
      set(sq, "purgeOnNoConsumers", false);
      set(sq, "exclusive", true);
      set(sq, "lastValue", true);
      set(sq, "lastValueKey", s("key"));
      set(sq, "nonDestructive", false);
      set(sq, "consumersBeforeDispatch", 0);
      set(sq, "delayBeforeDispatch", -1L);
      set(sq, "groupRebalance", true);
      set(sq, "groupBuckets", -1);
      set(sq, "autoDelete", false);
      set(sq, "autoDeleteDelay", 0L);
      set(sq, "autoDeleteMessageCount", 7L);
      set(sq, "groupFirstKey", s("gfk"));
      set(sq, "ringSize", -1L);
      set(sq, "enabled", false);
      set(sq, "groupRebalancePauseDispatch", true);
      f.emit("CreateSharedQueueMessageV2", "s", 136, sq);
      CreateSharedQueueMessage_V2 sqNulls = new CreateSharedQueueMessage_V2();
      set(sqNulls, "address", addr);
      set(sqNulls, "queueName", s("shared.sub"));
      f.emit("CreateSharedQueueMessageV2~nulls", "s", 136, sqNulls);

      f.emit("SessionDeleteQueueMessage", "s", 136, new SessionDeleteQueueMessage(queue));

      // ---- queries
      f.emit("SessionQueueQueryMessage", "s", 136, new SessionQueueQueryMessage(queue));

      SessionQueueQueryResponseMessage qr = new SessionQueueQueryResponseMessage();
      set(qr, "exists", true);
      set(qr, "durable", true);
      set(qr, "temporary", false);
      set(qr, "consumerCount", 3);
      set(qr, "messageCount", 123_456_789_012L);
      set(qr, "filterString", filter);
      set(qr, "address", addr);
      set(qr, "name", queue);
      f.emit("SessionQueueQueryResponseMessage", "c", 129, qr);
      f.emit("SessionQueueQueryResponseMessage~nulls", "c", 129, new SessionQueueQueryResponseMessage());

      SessionQueueQueryResponseMessage_V2 qr2 = new SessionQueueQueryResponseMessage_V2();
      set(qr2, "exists", true);
      set(qr2, "consumerCount", 1);
      set(qr2, "messageCount", 9L);
      set(qr2, "address", addr);
      set(qr2, "name", queue);
      set(qr2, "autoCreateQueues", true);
      f.emit("SessionQueueQueryResponseMessageV2", "c", 129, qr2);

      SessionQueueQueryResponseMessage_V3 qr3 = new SessionQueueQueryResponseMessage_V3();
      set(qr3, "exists", true);
      set(qr3, "durable", true);
      set(qr3, "consumerCount", 2);
      set(qr3, "messageCount", 42L);
      set(qr3, "filterString", filter);
      set(qr3, "address", addr);
      set(qr3, "name", queue);
      set(qr3, "autoCreateQueues", true);
      set(qr3, "autoCreated", false);
      set(qr3, "purgeOnNoConsumers", true);
      set(qr3, "routingType", RoutingType.ANYCAST);
      set(qr3, "maxConsumers", -1);
      set(qr3, "exclusive", false);
      set(qr3, "lastValue", true);
      set(qr3, "defaultConsumerWindowSize", 1024 * 1024);
      set(qr3, "lastValueKey", s("_AMQ_LVQ_NAME"));
      set(qr3, "nonDestructive", false);
      set(qr3, "consumersBeforeDispatch", 0);
      set(qr3, "delayBeforeDispatch", -1L);
      set(qr3, "groupRebalance", false);
      set(qr3, "groupBuckets", -1);
      set(qr3, "autoDelete", true);
      set(qr3, "autoDeleteDelay", 0L);
      set(qr3, "autoDeleteMessageCount", 0L);
      set(qr3, "groupFirstKey", s("gfk"));
      set(qr3, "ringSize", -1L);
      set(qr3, "enabled", true);
      set(qr3, "groupRebalancePauseDispatch", false);
      set(qr3, "configurationManaged", true);
      f.emit("SessionQueueQueryResponseMessageV3", "c", 136, qr3);
      // The default constructor: MULTICAST and every nullable attribute null.
      f.emit("SessionQueueQueryResponseMessageV3~nulls", "c", 136, new SessionQueueQueryResponseMessage_V3());

      f.emit("SessionBindingQueryMessage", "s", 136, new SessionBindingQueryMessage(addr));

      List<SimpleString> names = new ArrayList<>();
      names.add(queue);
      names.add(s("orders.audit"));
      names.add(s("x".repeat(300)));
      f.emit("SessionBindingQueryResponseMessage", "c", 129, new SessionBindingQueryResponseMessage(true, names));
      f.emit("SessionBindingQueryResponseMessage~empty", "c", 129,
         new SessionBindingQueryResponseMessage(false, new ArrayList<>()));
      f.emit("SessionBindingQueryResponseMessageV2", "c", 129, new SessionBindingQueryResponseMessage_V2(true, names, true));
      f.emit("SessionBindingQueryResponseMessageV3", "c", 129,
         new SessionBindingQueryResponseMessage_V3(true, names, false, true));
      f.emit("SessionBindingQueryResponseMessageV4", "c", 130, new SessionBindingQueryResponseMessage_V4(
         true, names, true, true, false, 20, true, false, s("lvk"), true, 3, 250L));
      f.emit("SessionBindingQueryResponseMessageV4~nulls", "c", 130, new SessionBindingQueryResponseMessage_V4(
         false, new ArrayList<>(), false, false, true, -1, null, null, null, null, null, null));
      f.emit("SessionBindingQueryResponseMessageV5", "c", 136, new SessionBindingQueryResponseMessage_V5(
         true, names, true, false, false, -1, false, true, s("lvk"), false, 0, -1L, true, false));
      f.emit("SessionBindingQueryResponseMessageV5~nulls", "c", 136, new SessionBindingQueryResponseMessage_V5(
         false, new ArrayList<>(), true, true, true, 1, null, null, null, null, null, null, false, true));

      // ---- consumers
      f.emit("SessionCreateConsumerMessage", "s", 136,
         new SessionCreateConsumerMessage(17, queue, filter, 5, false, true));
      f.emit("SessionCreateConsumerMessage~v129", "s", 129,
         new SessionCreateConsumerMessage(18, queue, null, 0, true, false));
      f.emit("SessionCreateConsumerMessage~v130", "s", 130,
         new SessionCreateConsumerMessage(19, queue, null, -3, true, false));
      f.emit("SessionConsumerCloseMessage", "s", 136, new SessionConsumerCloseMessage(17));
      f.emit("SessionConsumerFlowCreditMessage", "s", 136, new SessionConsumerFlowCreditMessage(17, 1_048_576));
      f.emit("SessionForceConsumerDelivery", "s", 136, new SessionForceConsumerDelivery(17, 99));
      f.emit("DisconnectConsumerMessage", "c", 136, new DisconnectConsumerMessage(17));
      f.emit("DisconnectConsumerWithKillMessage", "c", 136, new DisconnectConsumerWithKillMessage(s("node-1")));
      f.emit("DisconnectConsumerWithKillMessage~null", "c", 136, new DisconnectConsumerWithKillMessage(null));

      // ---- acknowledgements and transactions
      f.emit("SessionAcknowledgeMessage", "s", 136, new SessionAcknowledgeMessage(17, 1_000_000_007L, true));
      f.emit("SessionIndividualAcknowledgeMessage", "s", 136,
         new SessionIndividualAcknowledgeMessage(17, 1_000_000_008L, false));
      f.emit("SessionExpireMessage", "s", 136, new SessionExpireMessage(17, 1_000_000_009L));
      f.emit("SessionCommitMessage", "s", 131, new SessionCommitMessage());
      f.emit("SessionCommitMessage~v129", "s", 129, new SessionCommitMessage());
      f.emit("SessionCommitMessageV2", "s", 132, set(new SessionCommitMessage_V2(), "correlationID", 77L));
      f.emit("SessionCommitMessageV2~default", "s", 136, new SessionCommitMessage_V2());
      f.emit("RollbackMessage", "s", 136, new RollbackMessage(true));
      f.emit("RollbackMessage~false", "s", 136, new RollbackMessage(false));

      // ---- session lifecycle and metadata
      f.emit("SessionCloseMessage", "s", 136, new SessionCloseMessage());
      f.emit("ReattachSessionMessage", "s", 136, new ReattachSessionMessage("5c1f2b9e-session-name", 41));
      f.emit("ReattachSessionResponseMessage", "c", 136, new ReattachSessionResponseMessage(40, true));
      f.emit("SessionAddMetaDataMessage", "s", 136, new SessionAddMetaDataMessage("jms-session", ""));
      f.emit("SessionAddMetaDataMessageV2", "s", 136,
         new SessionAddMetaDataMessageV2("jms-client-id", "client-ü-" + "y".repeat(5000), true));
      f.emit("SessionAddMetaDataMessageV2~noconfirm", "s", 136,
         new SessionAddMetaDataMessageV2("k", "v", false));
      f.emit("SessionUniqueAddMetaDataMessage", "s", 136, new SessionUniqueAddMetaDataMessage("jms-client-id", "client-1"));

      // ---- producer flow control
      f.emit("SessionRequestProducerCreditsMessage", "s", 136, new SessionRequestProducerCreditsMessage(65536, addr));
      f.emit("SessionProducerCreditsMessage", "c", 136, new SessionProducerCreditsMessage(65536, addr));
      f.emit("SessionProducerCreditsFailMessage", "c", 136, new SessionProducerCreditsFailMessage(1024, addr));

      // ---- 1.x send and receive
      f.emit("SessionSendMessage1X", "s", 127, new SessionSendMessage_1X(Fixtures.message(11), true, null));
      f.emit("SessionSendMessage1X~v121", "s", 121, new SessionSendMessage_1X(Fixtures.message(12), false, null));
      f.emit("SessionReceiveMessage1X", "c", 127, new SessionReceiveMessage_1X(23, Fixtures.message(13), 4));

      f.done();
   }
}
