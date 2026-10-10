// Fixtures for crates/saranga-core-protocol/src/packets/{connection,message}.rs.

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.DisconnectReason;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;

public class ConnectionFixtures {
   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);
      SimpleString node = SimpleString.of("node-1");

      f.emit("Ping", "s", 136, new Ping(60_000));
      f.emit("DisconnectMessage", "c", 136, new DisconnectMessage(node));
      f.emit("DisconnectMessage~null", "c", 136, new DisconnectMessage(null));
      f.emit("DisconnectMessageV2", "c", 136, new DisconnectMessage_V2(node, "scale-down"));

      Map<String, Object> params = new HashMap<>();
      params.put("host", "broker-2");
      params.put("port", 61616);
      params.put("sslEnabled", true);
      params.put("batchDelay", 5L);
      Map<String, Object> extra = new HashMap<>();
      extra.put("note", "extra");
      TransportConfiguration tc = new TransportConfiguration(
         "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory", params, "connector-name", extra);
      f.emit("DisconnectMessageV3", "c", 136,
         new DisconnectMessage_V3(node, DisconnectReason.REDIRECT, SimpleString.of("node-2"), tc));
      f.emit("DisconnectMessageV3~minimal", "c", 136,
         new DisconnectMessage_V3(null, null, null, null));

      f.emit("ActiveMQExceptionMessage", "c", 129, new ActiveMQExceptionMessage(
         ActiveMQExceptionType.QUEUE_DOES_NOT_EXIST.createException("no such queue")));
      f.emit("ActiveMQExceptionMessageV2", "c", 136, new ActiveMQExceptionMessage_V2(
         99, ActiveMQExceptionType.SECURITY_EXCEPTION.createException("denied")));
      f.emit("NullResponseMessage", "c", 129, new NullResponseMessage());
      f.emit("NullResponseMessageV2", "c", 136, new NullResponseMessage_V2(1234));
      f.emit("PacketsConfirmedMessage", "s", 136, new PacketsConfirmedMessage(77));

      f.emit("CreateSessionMessage", "s", 129, new CreateSessionMessage(
         "short", 11, 129, "user", "secret", 102_400, true, false, true, false, 1024, "default.addr"));
      f.emit("CreateSessionMessageV2", "s", 136, new CreateSessionMessage_V2(
         "a-session-name-with-a-medium-length", 11, 136, null, null, 102_400, false, true, true, true, -1, null, "client-1"));
      // A name over 0xfff chars uses the SimpleString form of writeString.
      f.emit("CreateSessionMessageV2~longname", "s", 136, new CreateSessionMessage_V2(
         "n".repeat(5000), 12, 136, "ü✓", null, 0, false, false, false, false, 0, null, null));
      f.emit("CreateSessionResponseMessage", "c", 136, new CreateSessionResponseMessage(136));

      f.emit("SessionStart", "s", 136, new PacketImpl(PacketImpl.SESS_START));
      f.emit("SessionStop", "s", 136, new PacketImpl(PacketImpl.SESS_STOP));
      f.emit("SessionXASuspend", "s", 136, new PacketImpl(PacketImpl.SESS_XA_SUSPEND));
      f.emit("SessionXAInDoubtXids", "s", 136, new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS));
      f.emit("SessionXAGetTimeout", "s", 136, new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT));

      f.emit("SessionSendMessage", "s", 129, new SessionSendMessage(Fixtures.message(1), true, null));
      f.emit("SessionSendMessageV2", "s", 133, new SessionSendMessage_V2(Fixtures.message(2), false, null));
      SessionSendMessage_V3 v3 = new SessionSendMessage_V3(Fixtures.message(3), true, null, 5);
      v3.setCorrelationID(4242);
      f.emit("SessionSendMessageV3", "s", 136, v3);
      f.emit("SessionReceiveMessage", "c", 136, new SessionReceiveMessage(9, Fixtures.message(4), 2));

      f.done();
   }
}
