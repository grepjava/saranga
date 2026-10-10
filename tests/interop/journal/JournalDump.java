// Loads an Artemis journal directory with the real JournalImpl and prints
// what it recovers, one line per record, in the format of
// `cargo run -p saranga-journal --example sample -- dump`.
//
//   java -cp '<artemis>/lib/*' JournalDump.java <dir> <prefix> <ext> <fileSize>
import java.io.File;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

public class JournalDump {
   public static void main(String[] args) throws Exception {
      File dir = new File(args[0]);
      int fileSize = Integer.parseInt(args[3]);
      NIOSequentialFileFactory ff = new NIOSequentialFileFactory(dir, 1);
      JournalImpl journal = new JournalImpl(fileSize, 2, 10, 0, 0, ff, args[1], args[2], 1);
      // As the broker's storage manager sets up and loads its journals.
      journal.setRemoveExtraFilesOnLoad(true);
      journal.start();
      List<RecordInfo> committed = new ArrayList<>();
      List<PreparedTransactionInfo> prepared = new ArrayList<>();
      List<Long> failed = new ArrayList<>();
      journal.load(committed, prepared, (tx, records, deletes) -> failed.add(tx));
      if (System.getenv("JOURNAL_DEBUG") != null) {
         System.err.println(journal.debug());
      }
      HexFormat hex = HexFormat.of();
      for (RecordInfo r : committed) {
         out(record(r, hex));
      }
      for (PreparedTransactionInfo p : prepared) {
         out("P tx=" + p.getId() + " extra=" + hex.formatHex(p.getExtraData()));
         for (RecordInfo r : p.getRecords()) {
            out("  " + record(r, hex));
         }
         for (RecordInfo r : p.getRecordsToDelete()) {
            out("  D id=" + r.id);
         }
      }
      for (Long tx : failed) {
         out("F tx=" + tx);
      }
      journal.stop();
   }

   static void out(String line) {
      System.out.print(line + "\n");
   }

   static String record(RecordInfo r, HexFormat hex) {
      return "R id=" + r.id + " type=" + r.getUserRecordType() + " update=" + r.isUpdate + " cc=" + r.compactCount + " data=" + hex.formatHex(r.data);
   }
}
