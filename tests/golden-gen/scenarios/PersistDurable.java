// Golden scenario "persist-durable": durable state left in the journal at a clean shutdown.
//   java -cp '<artemis>/lib/*' PersistDurable.java tcp://host:port
//
// Bindings journal: addresses and queues with non-default attributes created via
// the Core API, an address with both routing types, auto-created JMS queues and
// durable subscriptions. Message journal: unconsumed messages of every body type
// and priority, a duplicate ID, acks, rollback redeliveries (delivery count
// updates), a scheduled message, a committed transaction, LVQ and ring-queue
// removals. Non-persistent messages and the non-durable queue must not survive.
// Bodies and properties are fixed; IDs and timestamps vary between runs.

import java.util.EnumSet;
import javax.jms.*;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class PersistDurable {
    // 2100-01-01T00:00:00Z: far enough out that the message stays scheduled.
    static final long SCHEDULED_AT = 4_102_444_800_000L;
    static final long HUNDRED_YEARS = 100L * 365 * 24 * 3600 * 1000;

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        coreQueues(url);
        jms(url);
        System.out.println("persist-durable: ok");
    }

    // Queues and addresses with non-default attributes, plus traffic that exercises them.
    static void coreQueues(String url) throws Exception {
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory();
             ClientSession s = sf.createSession(false, true, true)) {
            s.start();

            // ADDRESS_BINDING with both routing types, one queue of each.
            s.createAddress(SimpleString.of("persist.both"), EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST), false);
            s.createQueue(QueueConfiguration.of("persist.both.any").setAddress("persist.both").setRoutingType(RoutingType.ANYCAST));
            s.createQueue(QueueConfiguration.of("persist.both.multi").setAddress("persist.both").setRoutingType(RoutingType.MULTICAST));

            s.createQueue(QueueConfiguration.of("persist.exclusive").setRoutingType(RoutingType.ANYCAST)
                .setExclusive(true).setMaxConsumers(1).setPurgeOnNoConsumers(false));
            s.createQueue(QueueConfiguration.of("persist.lvq").setRoutingType(RoutingType.ANYCAST)
                .setLastValue(true).setLastValueKey("lvkey"));
            s.createQueue(QueueConfiguration.of("persist.filtered").setRoutingType(RoutingType.ANYCAST)
                .setFilterString("colour = 'red'"));
            s.createQueue(QueueConfiguration.of("persist.nondestructive").setRoutingType(RoutingType.ANYCAST)
                .setNonDestructive(true));
            s.createQueue(QueueConfiguration.of("persist.ring").setRoutingType(RoutingType.ANYCAST)
                .setRingSize(3L));
            s.createQueue(QueueConfiguration.of("persist.groups").setRoutingType(RoutingType.ANYCAST)
                .setGroupRebalance(true).setGroupRebalancePauseDispatch(true).setGroupBuckets(4)
                .setGroupFirstKey("JMSXFirstInGroupID").setConsumersBeforeDispatch(0).setDelayBeforeDispatch(5000L));
            s.createQueue(QueueConfiguration.of("persist.misc").setRoutingType(RoutingType.ANYCAST)
                .setUser("golden-user").setConsumerPriority(7).setAutoDelete(false).setAutoDeleteDelay(60_000L)
                .setAutoDeleteMessageCount(5L).setEnabled(true));
            // Durable address, non-durable queue: its messages must not survive a restart.
            s.createAddress(SimpleString.of("persist.topic"), RoutingType.MULTICAST, false);
            s.createQueue(QueueConfiguration.of("persist.topic.volatile").setAddress("persist.topic")
                .setRoutingType(RoutingType.MULTICAST).setDurable(false));

            ClientProducer p = s.createProducer();
            send(s, p, "persist.both", RoutingType.ANYCAST, "both-anycast", null, null);
            send(s, p, "persist.both", RoutingType.MULTICAST, "both-multicast", null, null);
            send(s, p, "persist.exclusive", null, "exclusive-1", null, null);
            send(s, p, "persist.lvq", null, "lvq-a-1", "lvkey", "a");
            send(s, p, "persist.lvq", null, "lvq-b-1", "lvkey", "b");
            send(s, p, "persist.lvq", null, "lvq-a-2", "lvkey", "a");   // replaces lvq-a-1
            send(s, p, "persist.filtered", null, "filtered-red", "colour", "red");
            send(s, p, "persist.filtered", null, "filtered-blue", "colour", "blue");   // not routed
            send(s, p, "persist.nondestructive", null, "nondestructive-1", null, null);
            send(s, p, "persist.nondestructive", null, "nondestructive-2", null, null);
            for (int i = 1; i <= 5; i++) {
                send(s, p, "persist.ring", null, "ring-" + i, null, null);       // ring keeps 3..5
            }
            for (int i = 1; i <= 4; i++) {
                send(s, p, "persist.groups", null, "group-" + i, "_AMQ_GROUP_ID", i % 2 == 0 ? "g-even" : "g-odd");
            }
            send(s, p, "persist.misc", null, "misc-1", null, null);

            // Non-destructive: consuming and acking leaves the message in the queue.
            try (ClientConsumer c = s.createConsumer("persist.nondestructive")) {
                ClientMessage m = c.receive(5_000);
                require(m != null, "receive persist.nondestructive");
                m.acknowledge();
            }
        }
    }

    static void send(ClientSession s, ClientProducer p, String address, RoutingType rt, String text,
                     String key, String value) throws Exception {
        ClientMessage m = s.createMessage(ClientMessage.TEXT_TYPE, true);
        m.getBodyBuffer().writeNullableSimpleString(SimpleString.of(text));
        if (rt != null) m.setRoutingType(rt);
        if (key != null) m.putStringProperty(key, value);
        p.send(address, m);
    }

    static void jms(String url) throws Exception {
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?consumerWindowSize=0");
             Connection conn = cf.createConnection()) {
            conn.setClientID("persist-client");
            conn.start();
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Durable subscriptions first, so the topic sends below reach them.
            Topic topic = s.createTopic("persist.topic");
            s.createDurableSubscriber(topic, "sub-all").close();
            s.createDurableSubscriber(topic, "sub-red", "colour = 'red'", false).close();
            MessageConsumer nonDurable = s.createConsumer(topic);   // left open, never read

            // Anycast queue: every priority, every body type, every property type.
            Queue queue = s.createQueue("persist.queue");
            MessageProducer p = s.createProducer(queue);
            for (int prio = 0; prio <= 9; prio++) {
                Message m = body(s, prio);
                m.setIntProperty("seq", prio);
                m.setStringProperty("kind", "priority");
                p.send(m, DeliveryMode.PERSISTENT, prio, 0);
            }
            TextMessage typed = s.createTextMessage("all-property-types");
            setAllProperties(typed);
            p.send(typed);
            TextMessage ttl = s.createTextMessage("expires-in-100-years");
            p.send(ttl, DeliveryMode.PERSISTENT, 4, HUNDRED_YEARS);
            for (int i = 0; i < 2; i++) {   // the second send is a duplicate and is dropped
                TextMessage dup = s.createTextMessage("dup");
                dup.setStringProperty("_AMQ_DUPL_ID", "persist-dup-1");
                p.send(dup);
            }
            TextMessage sched = s.createTextMessage("scheduled-2100");
            sched.setLongProperty("_AMQ_SCHED_DELIVERY", SCHEDULED_AT);
            p.send(sched);
            p.send(s.createTextMessage("non-persistent"), DeliveryMode.NON_PERSISTENT, 4, 0);

            // Topic: durable subscriptions keep their copies, the non-durable ones lose them.
            MessageProducer tp = s.createProducer(topic);
            for (String colour : new String[] {"red", "blue", "green"}) {
                TextMessage m = s.createTextMessage("topic-" + colour);
                m.setStringProperty("colour", colour);
                tp.send(m);
            }
            TextMessage np = s.createTextMessage("topic-non-persistent");
            np.setStringProperty("colour", "red");
            tp.send(np, DeliveryMode.NON_PERSISTENT, 4, 0);

            // Individual acks: 5 sent, 1st and 3rd acked, 2nd received but never acked.
            Session ind = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
            Queue acked = ind.createQueue("persist.acked");
            MessageProducer ap = ind.createProducer(acked);
            for (int i = 1; i <= 5; i++) {
                ap.send(ind.createTextMessage("acked-" + i));
            }
            MessageConsumer ac = ind.createConsumer(acked);
            Message a1 = ac.receive(5_000);
            Message a2 = ac.receive(5_000);
            Message a3 = ac.receive(5_000);
            require(a1 != null && a2 != null && a3 != null, "receive persist.acked");
            a1.acknowledge();
            a3.acknowledge();
            ac.close();
            ind.close();   // a2 goes back to the queue

            // Rollback redelivery: delivery counts 2 and 1 on the first two messages.
            Session tx = conn.createSession(true, Session.SESSION_TRANSACTED);
            Queue redeliver = tx.createQueue("persist.redeliver");
            MessageProducer rp = tx.createProducer(redeliver);
            for (int i = 1; i <= 3; i++) {
                rp.send(tx.createTextMessage("redeliver-" + i));
            }
            tx.commit();
            MessageConsumer rc = tx.createConsumer(redeliver);
            require(rc.receive(5_000) != null && rc.receive(5_000) != null, "receive persist.redeliver");
            tx.rollback();
            require(rc.receive(5_000) != null, "receive persist.redeliver again");
            tx.rollback();
            rc.close();

            // Committed transaction with sends and an ack.
            Queue txq = tx.createQueue("persist.tx");
            MessageProducer txp = tx.createProducer(txq);
            txp.send(tx.createTextMessage("tx-before"));
            tx.commit();
            MessageConsumer txc = tx.createConsumer(txq);
            require(txc.receive(5_000) != null, "receive persist.tx");
            txp.send(tx.createTextMessage("tx-1"));
            txp.send(tx.createTextMessage("tx-2"));
            tx.commit();
            txc.close();
            tx.close();

        }
    }

    static Message body(Session s, int n) throws JMSException {
        switch (n % 5) {
            case 0: return s.createTextMessage("text-" + n);
            case 1: {
                BytesMessage m = s.createBytesMessage();
                m.writeBytes(new byte[] {(byte) n, 0, (byte) 0x80, (byte) 0xff});
                return m;
            }
            case 2: {
                MapMessage m = s.createMapMessage();
                m.setString("name", "map-" + n);
                m.setInt("n", n);
                m.setBoolean("flag", true);
                m.setDouble("d", 0.5);
                m.setBytes("raw", new byte[] {1, 2, 3});
                return m;
            }
            case 3: {
                StreamMessage m = s.createStreamMessage();
                m.writeString("stream-" + n);
                m.writeInt(n);
                return m;
            }
            default: return s.createMessage();   // empty body
        }
    }

    static void setAllProperties(Message m) throws JMSException {
        m.setBooleanProperty("pBool", false);
        m.setByteProperty("pByte", (byte) 127);
        m.setShortProperty("pShort", (short) -32768);
        m.setIntProperty("pInt", 123456789);
        m.setLongProperty("pLong", Long.MIN_VALUE);
        m.setFloatProperty("pFloat", -0.25f);
        m.setDoubleProperty("pDouble", 1e300);
        m.setStringProperty("pString", "persist ✓");
        m.setStringProperty("pEmpty", "");
        m.setJMSCorrelationID("persist-corr-1");
        m.setJMSType("persist-type");
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
