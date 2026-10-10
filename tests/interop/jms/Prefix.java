// Phase 6 interop suite: acceptor address prefixes.
//   java -cp '<artemis>/lib/*' Prefix.java tcp://host:port [test-name...]
// The broker runs tests/interop/prefix/broker.xml, whose acceptor has
// anycastPrefix=jms.queue. and multicastPrefix=jms.topic.,mt.:
//   CONFIG=tests/interop/prefix/broker.xml SUITE=Prefix scripts/interop.sh [saranga|artemis]
// Run it against the reference Java broker too: a test that fails there is a wrong test.

import java.util.*;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

public class Prefix {
    static String url;
    static final String RUN = Long.toString(System.currentTimeMillis() % 1_000_000, 36);

    interface Test { void run(ClientSession s) throws Exception; }

    public static void main(String[] args) throws Exception {
        url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        Set<String> only = new HashSet<>(Arrays.asList(args).subList(Math.min(1, args.length), args.length));
        Map<String, Test> tests = new LinkedHashMap<>();
        tests.put("sendAnycastPrefix", Prefix::sendAnycastPrefix);
        tests.put("sendMulticastPrefix", Prefix::sendMulticastPrefix);
        tests.put("createQueuePrefixed", Prefix::createQueuePrefixed);
        tests.put("createAddressPrefixed", Prefix::createAddressPrefixed);
        tests.put("sharedQueuePrefixed", Prefix::sharedQueuePrefixed);
        tests.put("unprefixedStillWorks", Prefix::unprefixedStillWorks);

        int failed = 0;
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url + "?reconnectAttempts=0");
             ClientSessionFactory sf = locator.createSessionFactory()) {
            for (Map.Entry<String, Test> t : tests.entrySet()) {
                if (!only.isEmpty() && !only.contains(t.getKey())) continue;
                try (ClientSession s = sf.createSession()) {
                    s.start();
                    t.getValue().run(s);
                    System.out.printf("PASS %-30s%n", t.getKey());
                } catch (Throwable e) {
                    failed++;
                    System.out.printf("FAIL %-30s %s%n", t.getKey(), e);
                }
            }
        }
        System.out.println(failed == 0 ? "ALL PASSED" : failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static SimpleString ss(String s) {
        return SimpleString.of(s);
    }

    static String sendAndReceive(ClientSession s, String sendTo, String consumeFrom) throws Exception {
        String body = "prefix " + System.nanoTime();
        ClientMessage m = s.createMessage(false);
        m.getBodyBuffer().writeString(body);
        s.createProducer(sendTo).send(m);
        try (ClientConsumer c = s.createConsumer(consumeFrom)) {
            ClientMessage r = c.receive(5000);
            check(r != null, "nothing received from " + consumeFrom);
            check(body.equals(r.getBodyBuffer().readString()), "wrong body");
            r.acknowledge();
            return r.getAddress();
        }
    }

    // A send to jms.queue.X auto-creates the anycast queue X; the message
    // arrives addressed to X.
    static void sendAnycastPrefix(ClientSession s) throws Exception {
        String q = "p.any." + RUN;
        // The consumer may name the queue with the prefix too.
        String address = sendAndReceive(s, "jms.queue." + q, "jms.queue." + q);
        check(address.equals(q), "delivered to " + address);
        ClientSession.QueueQuery query = s.queueQuery(ss(q));
        check(query.isExists(), q + " not created");
        check(query.getRoutingType() == RoutingType.ANYCAST, "routing " + query.getRoutingType());
        check(query.getAddress().toString().equals(q), "address " + query.getAddress());
    }

    // A send to mt.X (the second multicast prefix) reaches a subscription
    // on address X.
    static void sendMulticastPrefix(ClientSession s) throws Exception {
        String a = "p.multi." + RUN;
        s.createAddress(ss(a), RoutingType.MULTICAST, false);
        s.createQueue(QueueConfiguration.of(a + ".sub").setAddress(a).setRoutingType(RoutingType.MULTICAST).setDurable(false));
        String address = sendAndReceive(s, "mt." + a, a + ".sub");
        check(address.equals(a), "delivered to " + address);
    }

    // createQueue: the address's prefix sets the routing type; address and
    // name lose their prefixes.
    static void createQueuePrefixed(ClientSession s) throws Exception {
        String q = "p.cq." + RUN;
        s.createQueue(QueueConfiguration.of("jms.queue." + q).setAddress("jms.queue." + q)
                          .setRoutingType(RoutingType.MULTICAST).setDurable(false));
        ClientSession.QueueQuery query = s.queueQuery(ss(q));
        check(query.isExists(), q + " not created");
        check(query.getRoutingType() == RoutingType.ANYCAST, "routing " + query.getRoutingType());
        check(query.getAddress().toString().equals(q), "address " + query.getAddress());
        check(s.queueQuery(ss("jms.queue." + q)).isExists(), "prefixed query does not find it");
        s.deleteQueue("jms.queue." + q);
        check(!s.queueQuery(ss(q)).isExists(), q + " not deleted");
    }

    // createAddress: a prefix gives the address that routing type alone.
    static void createAddressPrefixed(ClientSession s) throws Exception {
        String a = "p.ca." + RUN;
        s.createAddress(ss("jms.topic." + a), EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST), false);
        ClientSession.AddressQuery query = s.addressQuery(ss(a));
        check(query.isExists(), a + " not created");
        check(query.isSupportsMulticast() && !query.isSupportsAnycast(),
            "multicast " + query.isSupportsMulticast() + " anycast " + query.isSupportsAnycast());
        check(s.addressQuery(ss("jms.topic." + a)).isExists(), "prefixed query does not find it");
    }

    // createSharedQueue: only the address loses its prefix. The queue keeps
    // its prefixed name, which queue queries (stripping the prefix) cannot
    // find; the address's bindings show it.
    static void sharedQueuePrefixed(ClientSession s) throws Exception {
        String a = "p.sq." + RUN;
        String name = "jms.topic.shared." + RUN;
        s.createSharedQueue(QueueConfiguration.of(name).setAddress("jms.topic." + a)
                                .setRoutingType(RoutingType.MULTICAST).setDurable(false));
        check(!s.queueQuery(ss(name)).isExists(), "a queue query found it");
        List<SimpleString> names = s.addressQuery(ss(a)).getQueueNames();
        check(names.equals(List.of(ss(name))), "bindings of " + a + ": " + names);
    }

    static void unprefixedStillWorks(ClientSession s) throws Exception {
        String q = "p.plain." + RUN;
        s.createQueue(QueueConfiguration.of(q).setRoutingType(RoutingType.ANYCAST).setDurable(false));
        String address = sendAndReceive(s, q, q);
        check(address.equals(q), "delivered to " + address);
    }
}
