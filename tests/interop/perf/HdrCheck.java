import java.io.BufferedReader;
import java.io.FileReader;
import java.util.TreeMap;

import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;

/**
 * Reads a perf --hdr log with HdrHistogram's HistogramLogReader: its header
 * lines, then each tag with whether its intervals decoded and recorded
 * anything.
 */
public class HdrCheck {
   public static void main(String[] args) throws Exception {
      try (BufferedReader r = new BufferedReader(new FileReader(args[0]))) {
         System.out.println(r.readLine());
         System.out.println(r.readLine());
      }
      TreeMap<String, Long> counts = new TreeMap<>();
      HistogramLogReader reader = new HistogramLogReader(args[0]);
      EncodableHistogram h;
      while ((h = reader.nextIntervalHistogram()) != null) {
         counts.merge(String.valueOf(h.getTag()), ((Histogram) h).getTotalCount(), Long::sum);
      }
      reader.close();
      counts.forEach((tag, count) -> System.out.println("tag " + tag + " recorded " + (count > 0)));
   }
}
