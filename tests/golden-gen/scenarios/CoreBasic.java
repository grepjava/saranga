// Golden scenario "core-basic": run with the Artemis client jars on the classpath.
//   java -cp '<artemis>/lib/*' CoreBasic.java tcp://host:port
// Leaves messages on golden.q1 and golden.q2 so the journal keeps live records.

import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class CoreBasic {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
             Connection conn = cf.createConnection()) {
            conn.setClientID("golden-client");
            conn.start();

            // Plain auto-ack session: one of each message type, all property types.
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q1 = s.createQueue("golden.q1");
            MessageProducer p = s.createProducer(q1);

            TextMessage text = s.createTextMessage("hello saranga");
            setAllProperties(text);
            p.send(text);

            BytesMessage bytes = s.createBytesMessage();
            bytes.writeBytes(new byte[] {0, 1, 2, 3, (byte) 0xff});
            p.send(bytes);

            MapMessage map = s.createMapMessage();
            map.setString("k", "v");
            map.setInt("n", 42);
            p.send(map);

            StreamMessage stream = s.createStreamMessage();
            stream.writeBoolean(true);
            stream.writeLong(7L);
            p.send(stream);

            // Non-persistent, prioritised, expiring.
            p.send(s.createTextMessage("non-persistent"), DeliveryMode.NON_PERSISTENT, 9, 60_000);

            // Duplicate detection: the second send is dropped by the broker.
            for (int i = 0; i < 2; i++) {
                TextMessage dup = s.createTextMessage("dup");
                dup.setStringProperty("_AMQ_DUPL_ID", "golden-dup-1");
                p.send(dup);
            }

            // Scheduled delivery an hour out, so it stays in the journal.
            TextMessage sched = s.createTextMessage("scheduled");
            sched.setLongProperty("_AMQ_SCHED_DELIVERY", System.currentTimeMillis() + 3_600_000);
            p.send(sched);

            // Consume one message so the journal holds an ack.
            MessageConsumer c1 = s.createConsumer(q1);
            require(c1.receive(5_000) != null, "receive from golden.q1");
            c1.close();

            // Transacted session: one commit, one rollback.
            Session tx = conn.createSession(true, Session.SESSION_TRANSACTED);
            Queue q2 = tx.createQueue("golden.q2");
            MessageProducer tp = tx.createProducer(q2);
            tp.send(tx.createTextMessage("committed-1"));
            tp.send(tx.createTextMessage("committed-2"));
            tx.commit();
            tp.send(tx.createTextMessage("rolled-back"));
            tx.rollback();
            MessageConsumer tc = tx.createConsumer(q2);
            require(tc.receive(5_000) != null, "receive from golden.q2");
            tx.rollback(); // redelivery count goes up, the message stays
            tc.close();

            // Client ack.
            Session ca = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue q3 = ca.createQueue("golden.q3");
            ca.createProducer(q3).send(ca.createTextMessage("client-ack"));
            MessageConsumer cc = ca.createConsumer(q3);
            Message m = cc.receive(5_000);
            require(m != null, "receive from golden.q3");
            m.acknowledge();

            // Durable topic subscription with a selector, left with one pending message.
            Session ts = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic t = ts.createTopic("golden.topic");
            ts.createDurableSubscriber(t, "golden-sub", "colour = 'red'", false).close();
            MessageProducer tpub = ts.createProducer(t);
            TextMessage red = ts.createTextMessage("red");
            red.setStringProperty("colour", "red");
            tpub.send(red);
            TextMessage blue = ts.createTextMessage("blue");
            blue.setStringProperty("colour", "blue");
            tpub.send(blue);
        }
        System.out.println("core-basic: ok");
    }

    static void setAllProperties(Message m) throws JMSException {
        m.setBooleanProperty("pBool", true);
        m.setByteProperty("pByte", (byte) -1);
        m.setShortProperty("pShort", (short) 1234);
        m.setIntProperty("pInt", Integer.MIN_VALUE);
        m.setLongProperty("pLong", Long.MAX_VALUE);
        m.setFloatProperty("pFloat", 1.5f);
        m.setDoubleProperty("pDouble", -2.25);
        m.setStringProperty("pString", "ünïcødé ✓");
        m.setJMSCorrelationID("corr-1");
        m.setJMSType("golden-type");
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
