// Fixtures for crates/saranga-core-protocol/src/packets/large.rs.
//
// The packets are built the way the client (ActiveMQSessionContext) and the
// broker (CoreSessionCallback) build them. SESS_RECEIVE_LARGE_MSG is encoded
// by the broker as SessionReceiveLargeMessage around a LargeServerMessageImpl
// and decoded by the client as SessionReceiveClientLargeMessage, so its
// fixtures carry the latter name.

import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;

public class LargeFixtures {
   static byte[] chunk(int n) {
      byte[] b = new byte[n];
      for (int i = 0; i < n; i++) {
         b[i] = (byte) (i * 31 + 5);
      }
      return b;
   }

   /** A broker-side large message with the same headers as Fixtures.message. */
   static LargeServerMessageImpl serverLarge(long id) {
      LargeServerMessageImpl m = new LargeServerMessageImpl(null);
      CoreMessage src = Fixtures.message(id);
      m.setMessageID(id);
      m.setAddress(src.getAddressSimpleString());
      m.setType(src.getType());
      m.setDurable(true);
      m.setExpiration(src.getExpiration());
      m.setTimestamp(src.getTimestamp());
      m.setPriority(src.getPriority());
      m.setUserID(src.getUserID());
      m.putStringProperty("colour", "red");
      m.putIntProperty("n", 42);
      m.putLongProperty("_AMQ_LARGE_SIZE", 1_048_576L);
      return m;
   }

   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);

      // Client -> server: the initial headers packet, then the body chunks.
      f.emit("SessionSendLargeMessage", "s", 136, new SessionSendLargeMessage(Fixtures.message(10)));
      // A bare message: no address, no user ID, no properties.
      f.emit("SessionSendLargeMessage~bare", "s", 129, new SessionSendLargeMessage(new CoreMessage(11, 10)));

      CoreMessage msg = Fixtures.message(12);
      f.emit("SessionSendContinuationMessage", "s", 129,
         new SessionSendContinuationMessage(msg, chunk(100), true, false, 300, null));
      f.emit("SessionSendContinuationMessage~last", "s", 129,
         new SessionSendContinuationMessage(msg, chunk(37), false, true, 137, null));

      SessionSendContinuationMessage_V2 v2 =
         new SessionSendContinuationMessage_V2(msg, chunk(100), true, false, 300, null);
      v2.setCorrelationID(7001);
      f.emit("SessionSendContinuationMessageV2", "s", 133, v2);
      SessionSendContinuationMessage_V2 v2last =
         new SessionSendContinuationMessage_V2(msg, chunk(37), false, true, 137, null);
      v2last.setCorrelationID(7002);
      f.emit("SessionSendContinuationMessageV2~last", "s", 130, v2last);

      SessionSendContinuationMessage_V3 v3 =
         new SessionSendContinuationMessage_V3(msg, chunk(100), true, true, 300, 4, null);
      v3.setCorrelationID(8001);
      f.emit("SessionSendContinuationMessageV3", "s", 136, v3);
      SessionSendContinuationMessage_V3 v3last =
         new SessionSendContinuationMessage_V3(msg, chunk(37), false, true, 137, 4, null);
      v3last.setCorrelationID(8002);
      f.emit("SessionSendContinuationMessageV3~last", "s", 134, v3last);
      // An empty final chunk (body size an exact multiple of the chunk size).
      SessionSendContinuationMessage_V3 v3empty =
         new SessionSendContinuationMessage_V3(msg, new byte[0], false, false, 0, -1, null);
      v3empty.setCorrelationID(-1);
      f.emit("SessionSendContinuationMessageV3~empty", "s", 136, v3empty);

      // Server -> client: the initial headers packet, then the body chunks.
      f.emit("SessionReceiveClientLargeMessage", "c", 136,
         new SessionReceiveLargeMessage(21, serverLarge(13), 1_048_576L, 1));
      f.emit("SessionReceiveClientLargeMessage~plain", "c", 129,
         new SessionReceiveLargeMessage(22, Fixtures.message(14), 0, 3));
      f.emit("SessionReceiveContinuationMessage", "c", 136,
         new SessionReceiveContinuationMessage(21, chunk(200), true, false));
      f.emit("SessionReceiveContinuationMessage~last", "c", 136,
         new SessionReceiveContinuationMessage(21, chunk(1), false, false));

      f.done();
   }
}
