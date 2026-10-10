// Golden scenario "core-large": large messages through the Artemis JMS client.
//   java -cp '<artemis>/lib/*' CoreLarge.java tcp://host:port
// Sends messages above min-large-message-size (100 KiB by default) so they
// are streamed in chunks; one stays on the queue so the broker keeps a
// large-messages/*.msg file.

import java.util.Arrays;
import javax.jms.*;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class CoreLarge {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "tcp://localhost:61616";
        byte[] payload = new byte[150 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31 + 7);
        }
        // Small chunks so each message spans several continuation packets.
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?minLargeMessageSize=65536");
             Connection conn = cf.createConnection()) {
            conn.start();
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue("golden.large");
            MessageProducer p = s.createProducer(q);

            BytesMessage big = s.createBytesMessage();
            big.writeBytes(payload);
            big.setStringProperty("kind", "bytes");
            p.send(big);

            TextMessage text = s.createTextMessage("x".repeat(80_000));
            p.send(text);

            MessageConsumer c = s.createConsumer(q);
            BytesMessage got = (BytesMessage) c.receive(10_000);
            require(got != null, "receive large bytes");
            byte[] back = new byte[(int) got.getBodyLength()];
            got.readBytes(back);
            require(Arrays.equals(back, payload), "payload intact");
            c.close();
            // The large text message stays on the queue.
        }

        // Compressed large message.
        try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url + "?compressLargeMessages=true");
             Connection conn = cf.createConnection()) {
            Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[200 * 1024]);
            s.createProducer(s.createQueue("golden.large.compressed")).send(m);
        }
        System.out.println("core-large: ok");
    }

    static void require(boolean ok, String what) {
        if (!ok) throw new java.lang.IllegalStateException("failed: " + what);
    }
}
