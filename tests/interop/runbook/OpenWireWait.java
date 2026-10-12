// The runbook suite's OpenWire connection: the ActiveMQ 5 client that
// Artemis ships, consuming one message from a queue so that the connection
// stays while listConnections is called.
//   java -cp '<artemis>/lib/*' OpenWireWait.java tcp://host:port user password queue

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;

public class OpenWireWait {
   public static void main(String[] args) throws Exception {
      ConnectionFactory cf = new ActiveMQConnectionFactory(args[0]);
      try (Connection connection = cf.createConnection(args[1], args[2])) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(args[3]));
         System.out.println("connected");
         System.out.flush();
         Message message = consumer.receive(60000);
         System.out.println(message == null ? "nothing received" : "received");
      }
   }
}
