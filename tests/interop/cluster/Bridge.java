// The clients of the bridge suite: the Artemis JMS client (Core) and Qpid
// JMS (AMQP) sending to the source broker and receiving from the target,
// and management calls (JMSManagementHelper) on the source.
//   java -cp '<artemis>/lib/*' Bridge.java send <url> <queue> <from> <to>
//       durable text messages m<from>..m<to - 1>, each with property seq.
//   java -cp '<artemis>/lib/*' Bridge.java send-large <url> <queue> <bytes>
//       one durable bytes message of that size.
//   java -cp '<artemis>/lib/*' Bridge.java send-amqp <url> <queue> <count>
//       AMQP text messages with a JMS type and an application property.
//   java -cp '<artemis>/lib/*' Bridge.java receive <url> <queue> <count>
//       the messages, with what the bridge gave them; then that no more
//       come.
//   java -cp '<artemis>/lib/*' Bridge.java receive-amqp <url> <queue> <count>
//   java -cp '<artemis>/lib/*' Bridge.java names <url>
//       the bridges of the broker (getBridgeNames), sorted.
//   java -cp '<artemis>/lib/*' Bridge.java attributes <url> <bridge>
//   java -cp '<artemis>/lib/*' Bridge.java create <url> <json>
//   java -cp '<artemis>/lib/*' Bridge.java destroy <url> <name>
// tests/interop/cluster/bridge.sh runs it; it prints what it saw.

import java.util.*;
import javax.jms.*;

import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

public class Bridge {
    static final String USER = "admin", PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        String url = args[1];
        switch (args[0]) {
            case "send" -> send(url, args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
            case "send-large" -> sendLarge(url, args[2], Integer.parseInt(args[3]));
            case "send-amqp" -> sendAmqp(url, args[2], Integer.parseInt(args[3]));
            case "receive" -> receive(url, args[2], Integer.parseInt(args[3]));
            case "receive-amqp" -> receiveAmqp(url, args[2], Integer.parseInt(args[3]));
            case "names" -> names(url);
            case "attributes" -> attributes(url, args[2]);
            case "create" -> invoke(url, "broker", "createBridge", args[2]);
            case "destroy" -> invoke(url, "broker", "destroyBridge", args[2]);
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    static Connection core(String url) throws JMSException {
        return new ActiveMQConnectionFactory(url).createConnection(USER, PASSWORD);
    }

    static Connection amqp(String url) throws Exception {
        java.net.URI u = new java.net.URI(url);
        return new JmsConnectionFactory("amqp://" + u.getHost() + ":" + u.getPort()).createConnection(USER, PASSWORD);
    }

    static void send(String url, String queue, int from, int to) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (int i = from; i < to; i++) {
                TextMessage m = s.createTextMessage("m" + i);
                m.setIntProperty("seq", i);
                p.send(m);
            }
            System.out.println("sent " + (to - from));
        }
    }

    static void sendLarge(String url, String queue, int size) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(queue));
            byte[] body = new byte[size];
            for (int i = 0; i < size; i++) {
                body[i] = (byte) (i % 251);
            }
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(body);
            m.setStringProperty("kind", "large");
            p.send(m);
            System.out.println("sent a large message of " + size + " bytes");
        }
    }

    static void sendAmqp(String url, String queue, int count) throws Exception {
        try (Connection c = amqp(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = s.createProducer(s.createQueue(queue));
            for (int i = 0; i < count; i++) {
                TextMessage m = s.createTextMessage("a" + i);
                m.setJMSType("amqp-type");
                m.setStringProperty("color", i % 2 == 0 ? "red" : "blue");
                m.setJMSCorrelationID("corr-" + i);
                p.send(m);
            }
            System.out.println("sent " + count + " over AMQP");
        }
    }

    static void receive(String url, String queue, int count) throws Exception {
        try (Connection c = core(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            for (int i = 0; i < count; i++) {
                Message m = consumer.receive(30_000);
                if (m == null) {
                    System.out.println("received only " + i);
                    return;
                }
                System.out.println("received " + describe(m));
            }
            Message more = consumer.receive(1_000);
            System.out.println(more == null ? "no more" : "unexpected " + describe(more));
        }
    }

    static String describe(Message m) throws JMSException {
        String body;
        if (m instanceof TextMessage t) {
            body = t.getText();
        } else if (m instanceof BytesMessage b) {
            int size = (int) b.getBodyLength();
            byte[] bytes = new byte[size];
            b.readBytes(bytes);
            boolean ok = true;
            for (int i = 0; i < size; i++) {
                ok &= bytes[i] == (byte) (i % 251);
            }
            body = "bytes(" + size + (ok ? ", intact" : ", corrupt") + ")";
        } else {
            body = m.getClass().getSimpleName();
        }
        Object dup = m.getObjectProperty("_AMQ_BRIDGE_DUP");
        String seq = m.propertyExists("seq") ? " seq=" + m.getIntProperty("seq") : "";
        String type = m.getJMSType() != null ? " type=" + m.getJMSType() : "";
        String color = m.propertyExists("color") ? " color=" + m.getStringProperty("color") : "";
        String corr = m.getJMSCorrelationID() != null ? " correlation=" + m.getJMSCorrelationID() : "";
        return body + seq + type + color + corr
            + " duplicate-id=" + (dup instanceof byte[] b ? b.length + " bytes" : String.valueOf(dup));
    }

    static void receiveAmqp(String url, String queue, int count) throws Exception {
        try (Connection c = amqp(url)) {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue(queue));
            c.start();
            for (int i = 0; i < count; i++) {
                Message m = consumer.receive(30_000);
                if (m == null) {
                    System.out.println("received only " + i);
                    return;
                }
                String body = m instanceof TextMessage t ? t.getText() : m.getClass().getSimpleName();
                System.out.println("received over AMQP " + body + " type=" + m.getJMSType()
                    + " color=" + m.getStringProperty("color") + " correlation=" + m.getJMSCorrelationID());
            }
            Message more = consumer.receive(1_000);
            System.out.println(more == null ? "no more" : "unexpected more");
        }
    }

    static Object call(String url, java.util.function.Consumer<Message> fill) throws Exception {
        try (QueueConnection c = new ActiveMQConnectionFactory(url).createQueueConnection(USER, PASSWORD)) {
            QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueRequestor r = new QueueRequestor(s, s.createQueue("activemq.management"));
            c.start();
            Message m = s.createMessage();
            fill.accept(m);
            Message reply = r.request(m);
            Object result = JMSManagementHelper.getResult(reply);
            if (!JMSManagementHelper.hasOperationSucceeded(reply)) {
                throw new java.lang.IllegalStateException(String.valueOf(result));
            }
            return result;
        }
    }

    static void names(String url) throws Exception {
        Object[] names = (Object[]) call(url, m -> {
            try {
                JMSManagementHelper.putAttribute(m, "broker", "bridgeNames");
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });
        List<String> sorted = new ArrayList<>();
        for (Object n : names) {
            sorted.add(String.valueOf(n));
        }
        Collections.sort(sorted);
        System.out.println("bridges " + sorted);
    }

    static void attributes(String url, String bridge) throws Exception {
        for (String a : new String[] {"name", "queueName", "forwardingAddress", "filterString",
                "staticConnectors", "discoveryGroupName", "retryInterval", "retryIntervalMultiplier",
                "maxRetryInterval", "reconnectAttempts", "useDuplicateDetection", "HA",
                "transformerClassName", "started", "connected", "messagesAcknowledged",
                "messagesPendingAcknowledgement"}) {
            Object v = call(url, m -> {
                try {
                    JMSManagementHelper.putAttribute(m, "bridge." + bridge, a);
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println(a + " = " + (v instanceof Object[] arr ? Arrays.toString(arr) : v));
        }
    }

    static void invoke(String url, String resource, String operation, String arg) throws Exception {
        try {
            call(url, m -> {
                try {
                    JMSManagementHelper.putOperationInvocation(m, resource, operation, arg);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println(operation + " done");
        } catch (java.lang.IllegalStateException e) {
            System.out.println(operation + " failed: " + e.getMessage());
        }
    }
}
