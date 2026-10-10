// Fixtures for crates/saranga-core-protocol/src/packets/xa.rs.

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;

public class XaFixtures {
   static byte[] bytes(int n, int seed) {
      byte[] b = new byte[n];
      for (int i = 0; i < n; i++) {
         b[i] = (byte) (seed + i * 7);
      }
      return b;
   }

   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);
      // XidImpl(branchQualifier, formatId, globalTransactionId)
      Xid xid = new XidImpl(bytes(8, 1), 0x4a42, bytes(20, 100));
      Xid empty = new XidImpl(new byte[0], 0, new byte[0]);
      Xid max = new XidImpl(bytes(64, 3), -1, bytes(64, 200));

      f.emit("SessionXAStartMessage", "s", 136, new SessionXAStartMessage(xid));
      f.emit("SessionXAStartMessage~empty", "s", 129, new SessionXAStartMessage(empty));
      f.emit("SessionXAAfterFailedMessage", "s", 136, new SessionXAAfterFailedMessage(max));
      f.emit("SessionXAEndMessage", "s", 136, new SessionXAEndMessage(xid, false));
      f.emit("SessionXAEndMessage~failed", "s", 136, new SessionXAEndMessage(xid, true));
      f.emit("SessionXACommitMessage", "s", 136, new SessionXACommitMessage(xid, true));
      f.emit("SessionXACommitMessage~twophase", "s", 129, new SessionXACommitMessage(max, false));
      f.emit("SessionXAPrepareMessage", "s", 136, new SessionXAPrepareMessage(xid));
      f.emit("SessionXARollbackMessage", "s", 136, new SessionXARollbackMessage(max));
      f.emit("SessionXAJoinMessage", "s", 136, new SessionXAJoinMessage(xid));
      f.emit("SessionXAResumeMessage", "s", 136, new SessionXAResumeMessage(empty));
      f.emit("SessionXAForgetMessage", "s", 136, new SessionXAForgetMessage(xid));
      f.emit("SessionXASetTimeoutMessage", "s", 136, new SessionXASetTimeoutMessage(300));

      f.emit("SessionXAResponseMessage", "c", 129,
         new SessionXAResponseMessage(false, XAResource.XA_OK, null));
      f.emit("SessionXAResponseMessage~error", "c", 129,
         new SessionXAResponseMessage(true, XAException.XAER_NOTA, "Cannot find xid in resource manager"));
      f.emit("SessionXAResponseMessageV2", "c", 136,
         new SessionXAResponseMessage_V2(31337, false, XAResource.XA_RDONLY, null));
      f.emit("SessionXAResponseMessageV2~error", "c", 130,
         new SessionXAResponseMessage_V2(-1, true, XAException.XA_RBROLLBACK, "rolled back ✓"));

      List<Xid> xids = new ArrayList<>();
      xids.add(xid);
      xids.add(empty);
      xids.add(max);
      f.emit("SessionXAGetInDoubtXidsResponseMessage", "c", 136, new SessionXAGetInDoubtXidsResponseMessage(xids));
      f.emit("SessionXAGetInDoubtXidsResponseMessage~none", "c", 136,
         new SessionXAGetInDoubtXidsResponseMessage(new ArrayList<>()));
      f.emit("SessionXASetTimeoutResponseMessage", "c", 136, new SessionXASetTimeoutResponseMessage(true));
      f.emit("SessionXASetTimeoutResponseMessage~false", "c", 136, new SessionXASetTimeoutResponseMessage(false));
      f.emit("SessionXAGetTimeoutResponseMessage", "c", 136, new SessionXAGetTimeoutResponseMessage(60));

      f.done();
   }
}
