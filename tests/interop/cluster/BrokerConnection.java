// The clients of the AMQP broker connection suite, with the Artemis JMS
// client (Core):
//   java -cp '<artemis>/lib/*' BrokerConnection.java send <url> <queue> <count>
//       sends <count> messages to the queue.
//   java -cp '<artemis>/lib/*' BrokerConnection.java receive <url> <queue> <count>
//       receives up to <count> messages from the queue and prints them,
//       then whether any are left.
//   java -cp '<artemis>/lib/*' BrokerConnection.java consumers <url> <queue> <count>
//       waits for the queue to have <count> consumers.
//   java -cp '<artemis>/lib/*' BrokerConnection.java manage <url> <operation> [<argument>]
//       invokes the broker's management operation and prints its result.
//   java -cp '<artemis>/lib/*' BrokerConnection.java attribute <url> <resource> <attribute>
//       prints a management attribute.
// tests/interop/cluster/amqp-connection.sh runs it.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class BrokerConnection {
    static final String USER = "admin", PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "send" -> send(args[1], args[2], Integer.parseInt(args[3]));
            case "receive" -> receive(args[1], args[2], Integer.parseInt(args[3]));
            case "consumers" -> consumers(args[1], args[2], Integer.parseInt(args[3]));
            case "manage" -> System.out.println(manage(args[1], "broker", args[2],
                Arrays.copyOfRange(args, 3, args.length)));
            case "attribute" -> System.out.println(attribute(args[1], args[2], args[3]));
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static void send(String url, String queue, int count) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (int i = 0; i < count; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setIntProperty("n", i);
                p.send(m);
            }
            System.out.println("sent " + count + " to " + queue);
        }
    }

    static void receive(String url, String queue, int count) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            List<String> got = new ArrayList<>();
            Set<Integer> ns = new TreeSet<>();
            while (got.size() < count) {
                Message m = consumer.receive(15000);
                if (m == null) {
                    break;
                }
                got.add(((TextMessage) m).getText());
                ns.add(m.getIntProperty("n"));
            }
            System.out.println("received " + got);
            System.out.println("property n " + (ns.size() == got.size() ? "kept" : "LOST " + ns));
            System.out.println(consumer.receive(500) == null ? "none left" : "MORE");
        }
    }

    static void consumers(String url, String queue, int count) throws Exception {
        long deadline = System.currentTimeMillis() + 30000;
        Object n = null;
        while (System.currentTimeMillis() < deadline) {
            n = attribute(url, "queue." + queue, "consumerCount");
            if (n instanceof Number num && num.intValue() == count) {
                break;
            }
            Thread.sleep(200);
        }
        System.out.println(queue + " has " + n + " consumers");
    }

    static Object attribute(String url, String resource, String attribute) throws Exception {
        try (Connection c = core(url)) {
            QueueSession s = ((QueueConnection) c).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueRequestor requestor = new QueueRequestor(s, s.createQueue("activemq.management"));
            c.start();
            Message m = s.createMessage();
            JMSManagementHelper.putAttribute(m, resource, attribute);
            Message reply = requestor.request(m);
            return JMSManagementHelper.hasOperationSucceeded(reply)
                ? JMSManagementHelper.getResult(reply) : "FAILED " + JMSManagementHelper.getResult(reply);
        }
    }

    static Object manage(String url, String resource, String operation, String[] params) throws Exception {
        try (Connection c = core(url)) {
            QueueSession s = ((QueueConnection) c).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueRequestor requestor = new QueueRequestor(s, s.createQueue("activemq.management"));
            c.start();
            Message m = s.createMessage();
            JMSManagementHelper.putOperationInvocation(m, resource, operation, (Object[]) params);
            Message reply = requestor.request(m);
            Object result = JMSManagementHelper.getResult(reply);
            return JMSManagementHelper.hasOperationSucceeded(reply)
                ? result : "FAILED " + result;
        }
    }
}
