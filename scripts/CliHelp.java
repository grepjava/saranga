/*
 * Writes crates/saranga-cli/src/help/artemis-cli.json: the commands of the
 * Artemis CLI as picocli sees them (Artemis.buildCommand), the shell's
 * connect and disconnect among them, which saranga's `help` renders as
 * picocli's usage message.
 *
 *     DIST=reference/activemq-artemis/artemis-distribution/target/apache-artemis-2.57.0-bin/apache-artemis-2.57.0
 *     java -cp "$DIST/lib/*" scripts/CliHelp.java > crates/saranga-cli/src/help/artemis-cli.json
 *
 * Descriptions are written as declared, `${...}` and all: picocli
 * interpolates them as it prints them, and so does saranga. What picocli
 * completes each argument with is there too, for `saranga completion`.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.Shell;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.PositionalParamSpec;
import picocli.CommandLine.Model.UsageMessageSpec;

public class CliHelp {

   public static void main(String[] args) {
      CommandLine root = Artemis.buildCommand(true, true, true);
      // connect and disconnect, which only the shell's command line has,
      // after address as it adds them.
      Shell.setInShell();
      CommandLine shell = Artemis.buildCommand(true, true, false);
      CommandLine all = new CommandLine(root.getCommandSpec().userObject());
      for (Map.Entry<String, CommandLine> e : root.getSubcommands().entrySet()) {
         all.addSubcommand(e.getKey(), e.getValue());
         if (e.getKey().equals("address")) {
            all.addSubcommand("connect", shell.getSubcommands().get("connect"));
            all.addSubcommand("disconnect", shell.getSubcommands().get("disconnect"));
         }
      }
      all.setInterpolateVariables(false);
      StringBuilder out = new StringBuilder();
      command(out, all, "");
      System.out.println(out);
   }

   static void command(StringBuilder out, CommandLine line, String indent) {
      CommandSpec spec = line.getCommandSpec();
      UsageMessageSpec usage = spec.usageMessage();
      check(usage.sortOptions(), "sortOptions");
      check(usage.sortSynopsis(), "sortSynopsis");
      check(!usage.abbreviateSynopsis(), "abbreviateSynopsis");
      check(usage.customSynopsis().length == 0, "customSynopsis");
      check(usage.header().length == 0 && usage.footer().length == 0, "header/footer");
      check(!usage.showDefaultValues(), "showDefaultValues");
      out.append("{\"name\":").append(str(spec.name()));
      out.append(",\"hidden\":").append(usage.hidden());
      out.append(",\"description\":").append(strs(usage.description()));
      out.append(",\"options\":[");
      List<String> items = new ArrayList<>();
      for (OptionSpec o : spec.options()) {
         check(o.showDefaultValue() == CommandLine.Help.Visibility.ON_DEMAND, "showDefault of " + o.longestName());
         check(o.group() == null, "group of " + o.longestName());
         items.add("{\"names\":" + strs(o.names()) + arg(o) + "}");
      }
      out.append(String.join(",", items)).append("],\"parameters\":[");
      items.clear();
      for (PositionalParamSpec p : spec.positionalParameters()) {
         items.add("{\"index\":" + str(p.index().toString()) + arg(p) + "}");
      }
      out.append(String.join(",", items)).append("],\"commands\":[");
      items.clear();
      for (Map.Entry<String, CommandLine> e : line.getSubcommands().entrySet()) {
         check(e.getKey().equals(e.getValue().getCommandSpec().name()), "alias " + e.getKey());
         StringBuilder sub = new StringBuilder();
         command(sub, e.getValue(), indent + " ");
         items.add("\n" + indent + " " + sub);
      }
      out.append(String.join(",", items)).append("]}");
   }

   static String arg(ArgSpec a) {
      return ",\"label\":" + str(a.paramLabel()) + ",\"arity\":" + str(a.arity().toString())
         + ",\"multi\":" + a.isMultiValue() + ",\"hidden\":" + a.hidden() + ",\"required\":" + a.required()
         + ",\"description\":" + strs(a.description()) + candidates(a) + completion(a) + java(a);
   }

   /*
    * How picocli converts the argument's values (its element type's, for a
    * list), for saranga's parser: "boolean", "Boolean", "int", "long",
    * "String" or "File" for picocli's own converters, "enum" with the
    * constants for its enum converter, "upper-enum" with the enum's class
    * and constants for Artemis's ConnectionProtocol.ProtocolConverter
    * (valueOf of the value in upper case), and "list" when the values are
    * collected.
    */
   static String java(ArgSpec a) {
      Class<?> type = a.typeInfo().isMultiValue() ? a.typeInfo().getAuxiliaryTypes()[0] : a.type();
      StringBuilder out = new StringBuilder(",\"java\":");
      if (a.converters().length > 0) {
         check(a.converters().length == 1 && a.converters()[0].getClass().getName()
            .equals("org.apache.activemq.artemis.cli.commands.messages.ConnectionProtocol$ProtocolConverter"),
            "converter of " + a.paramLabel());
         List<String> l = new ArrayList<>();
         for (Object c : type.getEnumConstants()) {
            l.add(str(((Enum<?>) c).name()));
         }
         out.append("\"upper-enum\",\"class\":").append(str(type.getName()));
         out.append(",\"constants\":[").append(String.join(",", l)).append("]");
      } else if (type.isEnum()) {
         List<String> l = new ArrayList<>();
         for (Object c : type.getEnumConstants()) {
            l.add(str(((Enum<?>) c).name()));
         }
         out.append("\"enum\",\"constants\":[").append(String.join(",", l)).append("]");
      } else if (type == Boolean.TYPE || type == Boolean.class && a.initialValue() != null) {
         out.append("\"boolean\"");
      } else if (type == Boolean.class) {
         // Unset (null), true or false: Artemis's commands ask for a value
         // left unset.
         out.append("\"Boolean\"");
      } else if (type == Integer.TYPE || type == Integer.class) {
         out.append("\"int\"");
      } else if (type == Long.TYPE || type == Long.class) {
         out.append("\"long\"");
      } else if (type == String.class) {
         out.append("\"String\"");
      } else if (type == java.io.File.class) {
         out.append("\"File\"");
      } else {
         throw new IllegalStateException("not supported by saranga's parser: " + type + " of " + a.paramLabel());
      }
      if (a.typeInfo().isMultiValue()) {
         check(a.typeInfo().isCollection() || a.typeInfo().isArray(), "map of " + a.paramLabel());
         check(a.splitRegex().isEmpty(), "split of " + a.paramLabel());
         out.append(",\"list\":true");
      }
      return out.toString();
   }

   /*
    * What picocli's AutoComplete.bash completes the argument with: its
    * completion candidates (an enum's constants among them), and its type
    * when that is one it completes or treats as a flag.
    */
   static String completion(ArgSpec a) {
      StringBuilder out = new StringBuilder();
      if (a.completionCandidates() != null) {
         List<String> l = new ArrayList<>();
         for (String c : a.completionCandidates()) {
            l.add(str(c));
         }
         out.append(",\"completion\":[").append(String.join(",", l)).append("]");
      }
      Class<?> type = a.type();
      if (a.typeInfo().isMultiValue()) {
         type = a.typeInfo().getAuxiliaryTypes()[0];
      }
      if (a.type() == Boolean.TYPE || a.type() == Boolean.class) {
         out.append(",\"type\":\"boolean\"");
      } else if (type.equals(java.io.File.class) || type.getName().equals("java.nio.file.Path")) {
         out.append(",\"type\":\"file\"");
      } else if (type.equals(java.net.InetAddress.class)) {
         out.append(",\"type\":\"host\"");
      }
      return out.toString();
   }

   static String candidates(ArgSpec a) {
      if (a.completionCandidates() == null || !String.join("", a.description()).contains("${COMPLETION-CANDIDATES}")) {
         return "";
      }
      List<String> l = new ArrayList<>();
      for (String c : a.completionCandidates()) {
         l.add(str(c));
      }
      return ",\"candidates\":[" + String.join(",", l) + "]";
   }

   static void check(boolean ok, String what) {
      if (!ok) {
         throw new IllegalStateException("not supported by saranga's help: " + what);
      }
   }

   static String strs(String[] s) {
      List<String> l = new ArrayList<>();
      for (String x : s) {
         l.add(str(x));
      }
      return "[" + String.join(",", l) + "]";
   }

   static String str(String s) {
      StringBuilder b = new StringBuilder("\"");
      for (char c : s.toCharArray()) {
         switch (c) {
            case '"' -> b.append("\\\"");
            case '\\' -> b.append("\\\\");
            case '\n' -> b.append("\\n");
            default -> {
               if (c < 0x20 || c > 0x7e) {
                  b.append(String.format("\\u%04x", (int) c));
               } else {
                  b.append(c);
               }
            }
         }
      }
      return b.append('"').toString();
   }
}
