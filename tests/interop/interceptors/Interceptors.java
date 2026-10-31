// The interceptors of the interceptors suite, which interceptors.sh
// compiles into the Artemis instance's lib/. Saranga has the same, under
// the same class names, in crates/saranga-cli/src/interop_interceptors.rs
// (the CLI's interop-interceptors feature).
//
// For each protocol an incoming interceptor refuses what carries "refuse"
// and marks the rest with <protocol>_in=seen; an outgoing one refuses a
// delivery that carries "hide" and marks the rest with <protocol>_out=seen.
// MQTT's go by topic: "refuse" and "hide".

package org.saranga.interop;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireInterceptor;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrameInterceptor;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AmqpInterceptor;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.MessageDispatch;

public class Interceptors {

   static boolean markCore(Message m, String stop, String mark) {
      if (m.containsProperty(stop)) {
         return false;
      }
      m.putStringProperty(mark, "seen");
      return true;
   }

   public static class CoreIn implements Interceptor {
      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) {
         if (packet instanceof SessionSendMessage s) {
            return markCore(s.getMessage(), "refuse", "core_in");
         }
         return true;
      }
   }

   public static class CoreOut implements Interceptor {
      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) {
         if (packet instanceof SessionReceiveMessage r) {
            return markCore(r.getMessage(), "hide", "core_out");
         }
         return true;
      }
   }

   public static class StompIn implements StompFrameInterceptor {
      @Override
      public boolean intercept(StompFrame frame, RemotingConnection connection) {
         if (!frame.getCommand().equals("SEND")) {
            return true;
         }
         if (frame.hasHeader("refuse")) {
            return false;
         }
         frame.addHeader("stomp_in", "seen");
         return true;
      }
   }

   public static class StompOut implements StompFrameInterceptor {
      @Override
      public boolean intercept(StompFrame frame, RemotingConnection connection) {
         if (frame.getCommand().equals("MESSAGE") && frame.hasHeader("hide")) {
            return false;
         }
         // Every frame; a MESSAGE goes out as it was encoded before.
         frame.addHeader("stomp_out", "seen");
         return true;
      }
   }

   static boolean mqtt(MqttMessage m, String topic) {
      return !(m instanceof MqttPublishMessage p && p.variableHeader().topicName().equals(topic));
   }

   public static class MqttIn implements MQTTInterceptor {
      @Override
      public boolean intercept(MqttMessage m, RemotingConnection connection) {
         return mqtt(m, "refuse");
      }
   }

   public static class MqttOut implements MQTTInterceptor {
      @Override
      public boolean intercept(MqttMessage m, RemotingConnection connection) {
         return mqtt(m, "hide");
      }
   }

   static boolean markOpenWire(org.apache.activemq.command.Message m, String stop, String mark) throws Exception {
      if (m.getProperty(stop) != null) {
         return false;
      }
      m.setProperty(mark, "seen");
      return true;
   }

   public static class OpenWireIn implements OpenWireInterceptor {
      @Override
      public boolean intercept(Command command, RemotingConnection connection) {
         try {
            return !(command instanceof ActiveMQMessage m) || markOpenWire(m, "refuse", "ow_in");
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   public static class OpenWireOut implements OpenWireInterceptor {
      @Override
      public boolean intercept(Command command, RemotingConnection connection) {
         try {
            return !(command instanceof MessageDispatch d && d.getMessage() != null)
               || markOpenWire(d.getMessage(), "hide", "ow_out");
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   static boolean amqp(AMQPMessage m, String stop, String mark) {
      if (m.getObjectProperty(stop) != null) {
         return false;
      }
      m.putStringProperty(mark, "seen");
      return true;
   }

   public static class AmqpIn implements AmqpInterceptor {
      @Override
      public boolean intercept(AMQPMessage m, RemotingConnection connection) {
         return amqp(m, "refuse", "amqp_in");
      }
   }

   public static class AmqpOut implements AmqpInterceptor {
      @Override
      public boolean intercept(AMQPMessage m, RemotingConnection connection) {
         return amqp(m, "hide", "amqp_out");
      }
   }
}
