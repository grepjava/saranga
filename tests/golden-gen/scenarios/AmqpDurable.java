// Golden scenario "amqp-durable": AMQP messages left in the journal at a clean shutdown.
//   java -cp '<artemis>/lib/*' AmqpDurable.java tcp://host:port
//
// Qpid JMS sends durable messages of every JMS body type with properties of
// every type, to a queue and to a durable subscription. A large one is
// kept in a file. One message is
// scheduled, one has a time to live, one goes to the dead-letter address after
// rollbacks (the broker rewrites its AMQP encoding), one is acknowledged and
// one redelivered (a delivery count update). The journal then holds
// AMQPMessagePersisterV3 records as Artemis writes them, including the ones it
// changed. Bodies and properties are fixed; IDs and timestamps vary between runs.

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.jms.*;

import org.apache.qpid.jms.JmsConnectionFactory;

public class AmqpDurable {
    // 2100-01-01T00:00:00Z: far enough out that the message stays scheduled.
    static final long SCHEDULED_AT = 4_102_444_800_000L;
    static final long HUNDRED_YEARS = 100L * 365 * 24 * 3600 * 1000;

    public static void main(String[] args) throws Exception {
        URI u = new URI(args.length > 0 ? args[0] : "tcp://localhost:61616");
        String amqp = "amqp://" + u.getHost() + ":" + u.getPort();
        JmsConnectionFactory cf = new JmsConnectionFactory(amqp);

        try (Connection c = cf.createConnection()) {
            c.setClientID("amqp-golden");
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // The durable subscription first, so it gets the topic message.
            s.createDurableSubscriber(s.createTopic("amqp.golden.topic"), "golden-sub").close();

            Queue queue = s.createQueue("amqp.golden.queue");
            MessageProducer p = s.createProducer(queue);
            p.setDeliveryMode(DeliveryMode.PERSISTENT);

            TextMessage text = s.createTextMessage("amqp text");
            text.setStringProperty("s", "v");
            text.setIntProperty("i", 1);
            text.setLongProperty("l", 2L);
            text.setBooleanProperty("b", true);
            text.setDoubleProperty("d", 1.5);
            text.setFloatProperty("f", 2.5f);
            text.setShortProperty("sh", (short) 3);
            text.setByteProperty("by", (byte) 4);
            text.setJMSCorrelationID("golden-corr");
            text.setJMSType("golden-type");
            text.setJMSReplyTo(s.createQueue("amqp.golden.reply"));
            p.send(text, DeliveryMode.PERSISTENT, 7, 0);

            BytesMessage bytes = s.createBytesMessage();
            bytes.writeBytes(new byte[] {1, 2, 3, 4});
            p.send(bytes);
            MapMessage map = s.createMapMessage();
            map.setInt("n", 5);
            map.setString("t", "x");
            p.send(map);
            StreamMessage stream = s.createStreamMessage();
            stream.writeInt(6);
            stream.writeString("seven");
            p.send(stream);
            p.send(s.createObjectMessage(new ArrayList<>(List.of("object"))));
            p.send(s.createMessage());
            TextMessage grouped = s.createTextMessage("grouped");
            grouped.setStringProperty("JMSXGroupID", "golden-group");
            grouped.setIntProperty("JMSXGroupSeq", 1);
            p.send(grouped);
            p.send(s.createTextMessage("long-lived"), DeliveryMode.PERSISTENT, 4, HUNDRED_YEARS);

            // Over amqpMinLargeMessageSize: a large message in a file.
            BytesMessage large = s.createBytesMessage();
            byte[] body = new byte[150 * 1024];
            for (int i = 0; i < body.length; i++) {
                body[i] = (byte) i;
            }
            large.writeBytes(body);
            large.setStringProperty("large", "yes");
            MessageProducer largeProducer = s.createProducer(s.createQueue("amqp.golden.large"));
            largeProducer.send(large);

            MessageProducer delayed = s.createProducer(s.createQueue("amqp.golden.scheduled"));
            delayed.setDeliveryDelay(SCHEDULED_AT - System.currentTimeMillis());
            delayed.send(s.createTextMessage("scheduled"));

            MessageProducer topic = s.createProducer(s.createTopic("amqp.golden.topic"));
            topic.send(s.createTextMessage("for the subscription"));

            // Acknowledged: an ack record against an AMQP message.
            MessageProducer acked = s.createProducer(s.createQueue("amqp.golden.acked"));
            acked.send(s.createTextMessage("acked-1"));
            acked.send(s.createTextMessage("kept-2"));
            MessageConsumer ackCons = s.createConsumer(s.createQueue("amqp.golden.acked"));
            check(ackCons.receive(5000) != null, "acked-1");
            ackCons.close();
        }

        // Rolled back once (a delivery count update), then twice more, with
        // max-delivery-attempts 2 on amqp.golden.dlq: to the dead-letter address.
        try (Connection c = cf.createConnection()) {
            c.start();
            Session tx = c.createSession(true, Session.SESSION_TRANSACTED);
            tx.createProducer(tx.createQueue("amqp.golden.redelivered")).send(tx.createTextMessage("redelivered"));
            tx.createProducer(tx.createQueue("amqp.golden.dlq")).send(tx.createTextMessage("dead"));
            tx.commit();
            MessageConsumer r = tx.createConsumer(tx.createQueue("amqp.golden.redelivered"));
            check(r.receive(5000) != null, "redelivered");
            tx.rollback();
            r.close();
            MessageConsumer d = tx.createConsumer(tx.createQueue("amqp.golden.dlq"));
            for (int i = 0; i < 2; i++) {
                check(d.receive(5000) != null, "dead " + i);
                tx.rollback();
            }
            check(d.receive(500) == null, "moved to the DLQ");
            d.close();
        }
        System.out.println("amqp-durable: ok");
    }

    static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
