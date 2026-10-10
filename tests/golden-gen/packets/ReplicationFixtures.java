// Fixtures for crates/saranga-core-protocol/src/packets/replication.rs.
//
// Replication packets are decoded with ServerPacketDecoder on both ends of
// the replication connection, so they use "s" (plus a few "c" cases to pin
// that the dispatch does not depend on direction).

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.EncoderPersister;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalFileImpl;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq.artemis.core.message.impl.CoreMessagePersister;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager.JournalContent;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.DeliveryCountUpdateEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.RefEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.XidEncoding;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;
import org.apache.activemq.artemis.core.replication.ReplicationManager.ADD_OPERATION_TYPE;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersister;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessagePersisterV2;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.codec.ReadableBuffer;

public class ReplicationFixtures {
   static final byte BINDINGS = 0;
   static final byte MESSAGES = 1;

   // A header section (empty list) and an amqp-value body of "hello".
   static final byte[] AMQP_DATA = {
      0x00, 0x53, 0x70, 0x45,
      0x00, 0x53, 0x77, (byte) 0xA1, 0x05, 'h', 'e', 'l', 'l', 'o'};

   @SuppressWarnings({"unchecked", "rawtypes"})
   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);
      Persister messagePersister = CoreMessagePersister.getInstance();
      Persister encoder = EncoderPersister.getInstance();

      // Journal add/update: an operation boolean before 2.18 (131), a byte after.
      for (int v : new int[] {130, 131, 136}) {
         f.emit("ReplicationAddMessage~message-v" + v, "s", v, new ReplicationAddMessage(
            v < 131, MESSAGES, ADD_OPERATION_TYPE.ADD, 1001, JournalRecordIds.ADD_MESSAGE_PROTOCOL,
            messagePersister, Fixtures.message(1001)));
         f.emit("ReplicationAddMessage~update-v" + v, "s", v, new ReplicationAddMessage(
            v < 131, MESSAGES, ADD_OPERATION_TYPE.UPDATE, 1002, JournalRecordIds.UPDATE_DELIVERY_COUNT,
            encoder, new DeliveryCountUpdateEncoding(55, 3)));
      }
      f.emit("ReplicationAddMessage~event", "s", 136, new ReplicationAddMessage(
         false, MESSAGES, ADD_OPERATION_TYPE.EVENT, 1003, JournalRecordIds.ADD_REF,
         encoder, new RefEncoding(77)));
      f.emit("ReplicationAddMessage~empty", "c", 136, new ReplicationAddMessage(
         false, BINDINGS, ADD_OPERATION_TYPE.ADD, 1004, (byte) 21,
         encoder, new ByteArrayEncoding(new byte[0])));

      for (int v : new int[] {130, 136}) {
         f.emit("ReplicationAddTXMessage~add-v" + v, "s", v, new ReplicationAddTXMessage(
            v < 131, MESSAGES, ADD_OPERATION_TYPE.ADD, 500, 1005, JournalRecordIds.ADD_REF,
            encoder, new RefEncoding(78)));
         f.emit("ReplicationAddTXMessage~update-v" + v, "s", v, new ReplicationAddTXMessage(
            v < 131, MESSAGES, ADD_OPERATION_TYPE.UPDATE, 501, 1006, JournalRecordIds.ADD_MESSAGE_PROTOCOL,
            messagePersister, Fixtures.message(1006)));
      }
      f.emit("ReplicationAddTXMessage~event", "s", 136, new ReplicationAddTXMessage(
         false, MESSAGES, ADD_OPERATION_TYPE.EVENT, 502, 1007, JournalRecordIds.ACKNOWLEDGE_REF,
         encoder, new RefEncoding(79)));

      f.emit("ReplicationDeleteMessage", "s", 136, new ReplicationDeleteMessage(MESSAGES, 1008));
      f.emit("ReplicationDeleteTXMessage", "s", 136, new ReplicationDeleteTXMessage(
         MESSAGES, 503, 1009, new ByteArrayEncoding(new byte[] {1, 2, 3})));
      f.emit("ReplicationDeleteTXMessage~empty", "s", 136, new ReplicationDeleteTXMessage(
         BINDINGS, 504, 1010, new ByteArrayEncoding(new byte[0])));
      XidImpl xid = new XidImpl("branch".getBytes(), 42, "global-tx".getBytes());
      f.emit("ReplicationPrepareMessage", "s", 136, new ReplicationPrepareMessage(MESSAGES, 505, new XidEncoding(xid)));
      f.emit("ReplicationCommitMessage", "s", 136, new ReplicationCommitMessage(MESSAGES, false, 505));
      f.emit("ReplicationCommitMessage~rollback", "s", 136, new ReplicationCommitMessage(BINDINGS, true, 506));

      f.emit("ReplicationResponseMessage", "s", 136, new ReplicationResponseMessage());
      f.emit("ReplicationResponseMessage~client", "c", 136, new ReplicationResponseMessage());
      f.emit("ReplicationResponseMessageV2", "s", 136, new ReplicationResponseMessageV2(true));
      f.emit("ReplicationResponseMessageV2~false", "s", 136, new ReplicationResponseMessageV2(false));

      // Page writes: int page number before 2.24 (133), long plus address after.
      SimpleString addr = SimpleString.of("paged.address");
      long[] queues = {11, 12};
      f.emit("ReplicationPageWriteMessage~int", "s", 132, new ReplicationPageWriteMessage(
         new PagedMessageImpl(Fixtures.message(2001), queues, 600), 7, false, addr));
      f.emit("ReplicationPageWriteMessage~long", "s", 133, new ReplicationPageWriteMessage(
         new PagedMessageImpl(Fixtures.message(2002), queues, -1), 3_000_000_000L, true, addr));
      f.emit("ReplicationPageWriteMessage~noqueues", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(Fixtures.message(2003), new long[0], 601), 8, true, SimpleString.of("")));
      f.emit("ReplicationPageWriteMessage~client", "c", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(Fixtures.message(2004), queues, 602), 9, true, addr));

      LargeServerMessageImpl coreLarge = new LargeServerMessageImpl(new NullStorageManager());
      coreLarge.setMessageID(2005);
      coreLarge.setAddress("large.address");
      coreLarge.putStringProperty("k", "v");
      f.emit("ReplicationPageWriteMessage~corelarge", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(coreLarge, queues, 603), 10, true, addr));

      TypedProperties extra = new TypedProperties();
      extra.putSimpleStringProperty(SimpleString.of("x-extra"), SimpleString.of("yes"));
      AMQPStandardMessage amqpV3 = new AMQPStandardMessage(0, AMQP_DATA, extra);
      amqpV3.setMessageID(2006);
      amqpV3.setAddress("amqp.address");
      f.emit("ReplicationPageWriteMessage~amqpv3", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(amqpV3, queues, 604), 11, true, addr));
      AMQPStandardMessage amqpV2 = withPersister(AMQPMessagePersisterV2.getInstance(), null);
      amqpV2.setMessageID(2007);
      f.emit("ReplicationPageWriteMessage~amqpv2", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(amqpV2, queues, 605), 12, true, addr));
      AMQPStandardMessage amqpV1 = withPersister(AMQPMessagePersister.getInstance(), extra);
      amqpV1.setMessageID(2008);
      amqpV1.setAddress("amqp.v1");
      f.emit("ReplicationPageWriteMessage~amqpv1", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(amqpV1, queues, 606), 13, true, addr));

      AMQPLargeMessage amqpLarge = new AMQPLargeMessage(2009, 0, extra, null, new NullStorageManager());
      amqpLarge.setAddress("amqp.large");
      amqpLarge.setFileDurable(true);
      // A header section: durable, priority 5.
      amqpLarge.parseHeader(ReadableBuffer.ByteBufferReader.wrap(ByteBuffer.wrap(new byte[] {
         0x00, 0x53, 0x70, (byte) 0xC0, 0x04, 0x02, 0x41, 0x50, 0x05})));
      // getExpiration would otherwise scan the (absent) large message file.
      amqpLarge.reloadExpiration(1_800_000_000_000L);
      f.emit("ReplicationPageWriteMessage~amqplarge", "s", 136, new ReplicationPageWriteMessage(
         new PagedMessageImpl(amqpLarge, queues, 607), 14, true, addr));
      AMQPLargeMessage amqpLargeOld = new AMQPLargeMessage(2010, 0, null, null, new NullStorageManager());
      amqpLargeOld.reloadExpiration(0);
      f.emit("ReplicationPageWriteMessage~amqplargeint", "s", 132, new ReplicationPageWriteMessage(
         new PagedMessageImpl(amqpLargeOld, queues, 608), 15, false, null));

      f.emit("ReplicationPageEventMessage~int", "s", 132, new ReplicationPageEventMessage(
         SimpleString.of("store-a"), 5, false, false));
      f.emit("ReplicationPageEventMessage~long", "s", 133, new ReplicationPageEventMessage(
         SimpleString.of("store-b"), 4_000_000_000L, true, true));

      f.emit("ReplicationLargeMessageBeginMessage", "s", 136, new ReplicationLargeMessageBeginMessage(3001));
      f.emit("ReplicationLargeMessageEndMessage", "s", 136, new ReplicationLargeMessageEndMessage(3001, 77, false));
      f.emit("ReplicationLargeMessageEndMessage~delete", "s", 136, new ReplicationLargeMessageEndMessage(3002, -1, true));
      f.emit("ReplicationLargeMessageWriteMessage", "s", 136, new ReplicationLargeMessageWriteMessage(
         3001, "large body chunk".getBytes()));
      f.emit("ReplicationLargeMessageWriteMessage~empty", "s", 136, new ReplicationLargeMessageWriteMessage(3001, new byte[0]));

      // Initial synchronization.
      JournalFile[] files = {journalFile(41), journalFile(42)};
      f.emit("ReplicationStartSyncMessage~messages", "s", 136, new ReplicationStartSyncMessage(
         files, JournalContent.MESSAGES, "node-uuid-1", true));
      f.emit("ReplicationStartSyncMessage~bindings", "s", 136, new ReplicationStartSyncMessage(
         new JournalFile[0], JournalContent.BINDINGS, "node-uuid-1", false));
      f.emit("ReplicationStartSyncMessage~large", "s", 136, new ReplicationStartSyncMessage(List.of(51L, 52L, 53L)));
      f.emit("ReplicationStartSyncMessage~finished", "s", 136, new ReplicationStartSyncMessage("node-uuid-1", 9L));

      f.emit("ReplicationSyncFileMessage~journal", "s", 136, new ReplicationSyncFileMessage(
         JournalContent.MESSAGES, null, 41, 16, chunk(16, 16)));
      f.emit("ReplicationSyncFileMessage~bindings", "s", 136, new ReplicationSyncFileMessage(
         JournalContent.BINDINGS, null, 42, 8, chunk(8, 8)));
      // The last chunk: fewer bytes read than the buffer's writer index.
      f.emit("ReplicationSyncFileMessage~partial", "s", 136, new ReplicationSyncFileMessage(
         null, SimpleString.of("store-a"), 5, 10, chunk(32, 10)));
      f.emit("ReplicationSyncFileMessage~eof", "s", 136, new ReplicationSyncFileMessage(
         null, null, 3001, -1, chunk(32, 0)));
      f.emit("ReplicationSyncFileMessage~zero", "s", 136, new ReplicationSyncFileMessage(
         null, null, 3001, 0, chunk(32, 0)));
      f.emit("ReplicationSyncFileMessage~noid", "s", 136, new ReplicationSyncFileMessage(
         null, null, -1, 0, null));

      f.emit("ReplicationPrimaryIsStoppingMessage", "s", 136, new ReplicationPrimaryIsStoppingMessage(
         ReplicationPrimaryIsStoppingMessage.PrimaryStopping.STOP_CALLED));
      f.emit("ReplicationPrimaryIsStoppingMessage~failover", "s", 136, new ReplicationPrimaryIsStoppingMessage(
         ReplicationPrimaryIsStoppingMessage.PrimaryStopping.FAIL_OVER));

      f.done();
   }

   /** An AMQP message that pages with an older persister. */
   static AMQPStandardMessage withPersister(Persister<Message> persister, TypedProperties extra) {
      return new AMQPStandardMessage(0, AMQP_DATA, extra) {
         @Override
         public Persister<Message> getPersister() {
            return persister;
         }
      };
   }

   static JournalFile journalFile(long id) {
      SequentialFile file = (SequentialFile) Proxy.newProxyInstance(
         ReplicationFixtures.class.getClassLoader(), new Class<?>[] {SequentialFile.class},
         (proxy, method, args) -> {
            throw new UnsupportedOperationException(method.getName());
         });
      return new JournalFileImpl(file, id, 2);
   }

   /** A file read buffer with {@code capacity} bytes written, {@code filled} of them data. */
   static ByteBuf chunk(int capacity, int filled) {
      ByteBuf buf = Unpooled.buffer(capacity, capacity);
      byte[] bytes = new byte[capacity];
      for (int i = 0; i < filled; i++) {
         bytes[i] = (byte) (i * 7 + 1);
      }
      buf.writeBytes(bytes);
      return buf;
   }
}
