// Phase 6 exit check: a broker started from an unmodified `artemis create`
// etc/ directory serves clients as that configuration says.
//   java -cp '<artemis>/lib/*' EtcCheck.java tcp://host:port <user> <password> <queue> [wrong-password]
// Sends and receives a message on <queue> as <user> (no user: anonymous);
// with [wrong-password], also checks that it is refused.

import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class EtcCheck {
    public static void main(String[] args) throws Exception {
        String url = args[0];
        String user = args[1].isEmpty() ? null : args[1];
        String password = args[2].isEmpty() ? null : args[2];
        String queue = args[3];
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?reconnectAttempts=0;initialConnectAttempts=10;retryInterval=500");
        String body = "etc-check " + System.nanoTime();
        try (Connection c = user == null ? cf.createConnection() : cf.createConnection(user, password);
             Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            c.start();
            Queue q = s.createQueue(queue);
            s.createProducer(q).send(s.createTextMessage(body));
            Message m = s.createConsumer(q).receive(5000);
            if (m == null || !body.equals(((TextMessage) m).getText())) {
                throw new AssertionError("round trip on " + queue + " failed: " + m);
            }
        }
        if (args.length > 4) {
            try (Connection c = cf.createConnection(user, args[4])) {
                throw new AssertionError("a wrong password was accepted");
            } catch (JMSSecurityException expected) {
                // refused, as it should be
            }
        }
        System.out.println("OK");
    }
}
