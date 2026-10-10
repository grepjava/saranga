// Print what a Core client sees on a recovered broker, one deterministic line per item.
//   java -cp '<artemis>/lib/*' DumpQueues.java tcp://host:port <queue>...
//
// Uses only the Core client API (no management). For each queue, in argument order:
// the QueueQuery result, then every message drained with receiveImmediate() and
// acknowledged. A queue given as address::queue (FQQN) is queried by its queue part
// and consumed through the FQQN. Before the queues it lists the in-doubt XA
// transactions from XAResource.recover() and leaves them unresolved.
//
// Scheduled messages are not delivered until their time, so they only show in the
// query's messageCount. Bodies up to 256 bytes print as hex, longer ones as length
// and SHA-256. Output is UTF-8 with \n line endings.

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

public class DumpQueues {
    static final int BODY_HEX_LIMIT = 256;
    static PrintStream out;

    public static void main(String[] args) throws Exception {
        out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8);
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
             ClientSessionFactory sf = locator.createSessionFactory()) {
            dumpXids(sf);
            try (ClientSession s = sf.createSession(false, true, true)) {
                s.start();
                for (int i = 1; i < args.length; i++) {
                    dumpQueue(s, args[i]);
                }
            }
        }
        out.flush();
    }

    static void dumpXids(ClientSessionFactory sf) throws Exception {
        try (ClientSession xs = sf.createSession(true, false, false)) {
            Xid[] xids = xs.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            TreeSet<String> lines = new TreeSet<>();
            for (Xid x : xids) {
                lines.add("xid formatId=" + x.getFormatId() + " gtrid=" + hex(x.getGlobalTransactionId())
                    + " bqual=" + hex(x.getBranchQualifier()));
            }
            line("xids count=" + lines.size());
            for (String l : lines) {
                line(l);
            }
        }
    }

    static void dumpQueue(ClientSession s, String spec) throws Exception {
        int sep = spec.indexOf("::");
        String queueName = sep >= 0 ? spec.substring(sep + 2) : spec;
        ClientSession.QueueQuery q = s.queueQuery(SimpleString.of(queueName));
        if (!q.isExists()) {
            line("queue " + spec + " exists=false");
            return;
        }
        line("queue " + spec + " exists=true"
            + " address=" + q.getAddress()
            + " routingType=" + q.getRoutingType()
            + " filter=" + q.getFilterString()
            + " durable=" + q.isDurable()
            + " temporary=" + q.isTemporary()
            + " autoCreated=" + q.isAutoCreated()
            + " consumers=" + q.getConsumerCount()
            + " messageCount=" + q.getMessageCount()
            + " maxConsumers=" + q.getMaxConsumers()
            + " purgeOnNoConsumers=" + q.isPurgeOnNoConsumers()
            + " exclusive=" + q.isExclusive()
            + " lastValue=" + q.isLastValue()
            + " lastValueKey=" + q.getLastValueKey()
            + " nonDestructive=" + q.isNonDestructive()
            + " consumersBeforeDispatch=" + q.getConsumersBeforeDispatch()
            + " delayBeforeDispatch=" + q.getDelayBeforeDispatch()
            + " groupRebalance=" + q.isGroupRebalance()
            + " groupRebalancePauseDispatch=" + q.isGroupRebalancePauseDispatch()
            + " groupBuckets=" + q.getGroupBuckets()
            + " groupFirstKey=" + q.getGroupFirstKey()
            + " autoDelete=" + q.isAutoDelete()
            + " autoDeleteDelay=" + q.getAutoDeleteDelay()
            + " autoDeleteMessageCount=" + q.getAutoDeleteMessageCount()
            + " ringSize=" + q.getRingSize()
            + " enabled=" + q.isEnabled()
            + " configurationManaged=" + q.isConfigurationManaged());

        int n = 0;
        try (ClientConsumer c = s.createConsumer(spec)) {
            ClientMessage m;
            while ((m = c.receiveImmediate()) != null) {
                line(describe(spec, m));
                m.acknowledge();
                n++;
            }
        }
        line("drained " + spec + " messages=" + n);
    }

    static String describe(String queue, ClientMessage m) throws Exception {
        StringBuilder b = new StringBuilder("msg ").append(queue)
            .append(" id=").append(m.getMessageID())
            .append(" userID=").append(m.getUserID())
            .append(" address=").append(m.getAddress())
            .append(" durable=").append(m.isDurable())
            .append(" priority=").append(m.getPriority())
            .append(" deliveryCount=").append(m.getDeliveryCount())
            .append(" timestamp=").append(m.getTimestamp())
            .append(" expiration=").append(m.getExpiration())
            .append(" large=").append(m.isLargeMessage())
            .append(" props={");
        TreeSet<String> names = new TreeSet<>();
        for (SimpleString k : m.getPropertyNames()) {
            names.add(k.toString());
        }
        boolean first = true;
        for (String k : names) {
            if (!first) b.append(", ");
            first = false;
            b.append(k).append(':').append(typed(m.getObjectProperty(SimpleString.of(k))));
        }
        b.append("} type=").append(typeName(m.getType()));

        ActiveMQBuffer buf = m.getBodyBuffer();
        byte[] body = new byte[buf.readableBytes()];
        buf.readBytes(body);
        b.append(" body=").append(body.length).append(':');
        if (body.length <= BODY_HEX_LIMIT) {
            b.append(hex(body));
        } else {
            b.append("sha256=").append(hex(MessageDigest.getInstance("SHA-256").digest(body)));
        }
        if (m.getType() == Message.TEXT_TYPE && body.length > 0) {
            SimpleString text = ActiveMQBuffers.wrappedBuffer(body).readNullableSimpleString();
            if (text == null) {
                b.append(" text=null");
            } else if (text.length() <= BODY_HEX_LIMIT) {
                b.append(" text=\"").append(text).append('"');
            } else {
                b.append(" textLength=").append(text.length());
            }
        }
        return b.toString();
    }

    static String typed(Object v) {
        if (v == null) return "null";
        if (v instanceof byte[] bytes) return "bytes=" + hex(bytes);
        String type = v instanceof SimpleString ? "string" : v.getClass().getSimpleName().toLowerCase();
        return type + "=" + v;
    }

    static String typeName(byte t) {
        return switch (t) {
            case Message.DEFAULT_TYPE -> "DEFAULT";
            case Message.OBJECT_TYPE -> "OBJECT";
            case Message.TEXT_TYPE -> "TEXT";
            case Message.BYTES_TYPE -> "BYTES";
            case Message.MAP_TYPE -> "MAP";
            case Message.STREAM_TYPE -> "STREAM";
            default -> "TYPE" + t;
        };
    }

    static String hex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder b = new StringBuilder(bytes.length * 2);
        for (byte x : bytes) {
            b.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        }
        return b.toString();
    }

    static void line(String s) {
        out.print(s);
        out.print('\n');
    }
}
