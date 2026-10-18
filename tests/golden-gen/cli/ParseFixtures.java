/*
 * Writes tests/golden/cli/parse.txt: command lines parsed by the Artemis
 * CLI's own picocli command line (Artemis.buildCommand, as internalExecute
 * builds it from an instance and from the home, and as the shell builds it
 * in each), and what picocli makes of each: the ParameterException's
 * message Artemis prints, the command that threw it (whose usage the shell
 * prints) and the suggestions the shell prints instead of the usage
 * (UnmatchedArgumentException.printSuggestions), or the command matched and
 * the typed value of each option and positional parameter given. saranga's
 * parser (crates/saranga-cli/src/help/parse.rs) must make the same of every
 * line.
 *
 *     DIST=reference/activemq-artemis/artemis-distribution/target/apache-artemis-2.57.0-bin/apache-artemis-2.57.0
 *     java -cp "$DIST/lib/*" tests/golden-gen/cli/ParseFixtures.java > tests/golden/cli/parse.txt
 *
 * The cases come from the model: for each command, an unknown option and
 * unmatched arguments, and for each option the forms picocli accepts and
 * refuses (a missing value, a value of the wrong type, the value attached
 * with '=', an option given twice, a flag given a value). A line is
 *
 *     <mode> <arg> <arg> ...  =>  error <message>  @  <command path>  ?  <suggestions>
 *     <mode> <arg> <arg> ...  =>  ok <command path> [<name>=<value>]...
 *
 * with the arguments separated by U+001F (an empty one shows as nothing
 * between two separators, or after the space), the root's command path
 * "-", no suggestions nothing, and the mode "instance", "home", "shell"
 * (in the shell, from an instance) or "shell-home", alone when there are
 * no arguments. In the shell only lines whose first word is a command of
 * the shell are written: JLine answers any other with "Unknown command"
 * before picocli parses it.
 */

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.Shell;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.PositionalParamSpec;
import picocli.CommandLine.ParseResult;

public class ParseFixtures {

   static final String SEP = "\u001f";

   public static void main(String[] args) throws Exception {
      StringBuilder out = new StringBuilder();
      // The shell's modes last: Shell.setInShell cannot be undone.
      for (String mode : new String[] {"instance", "home", "shell", "shell-home"}) {
         boolean instance = mode.equals("instance") || mode.equals("shell");
         boolean shell = mode.startsWith("shell");
         if (shell) {
            Shell.setInShell();
         }
         Set<List<String>> cases = new LinkedHashSet<>();
         root(cases);
         CommandLine line = build(instance, shell);
         walk(cases, line, new ArrayList<>());
         for (List<String> c : cases) {
            if (shell && (c.isEmpty() || !line.getSubcommands().containsKey(c.get(0)))) {
               continue;
            }
            out.append(mode);
            if (!c.isEmpty()) {
               out.append(' ').append(String.join(SEP, c));
            }
            out.append("  =>  ").append(parse(instance, shell, c)).append('\n');
         }
      }
      System.out.print(out);
   }

   /** The command line of internalExecute, or of the shell. */
   static CommandLine build(boolean instance, boolean shell) {
      return Artemis.buildCommand(instance, !instance, !shell);
   }

   /** What a fresh command line (fields at their defaults) makes of args. */
   static String parse(boolean instance, boolean shell, List<String> args) {
      CommandLine line = build(instance, shell);
      try {
         ParseResult r = line.parseArgs(args.toArray(new String[0]));
         List<String> path = new ArrayList<>();
         while (r.hasSubcommand()) {
            r = r.subcommand();
            path.add(r.commandSpec().name());
         }
         StringBuilder b = new StringBuilder("ok ").append(path.isEmpty() ? "-" : String.join(" ", path));
         // A multi-value argument is matched once per value given.
         Set<ArgSpec> seen = new java.util.HashSet<>();
         for (OptionSpec o : r.matchedOptions()) {
            if (seen.add(o)) {
               b.append(' ').append(o.longestName()).append('=').append(value(o));
            }
         }
         for (PositionalParamSpec p : r.matchedPositionals()) {
            if (seen.add(p)) {
               b.append(" [").append(p.index()).append("]=").append(value(p));
            }
         }
         return b.toString();
      } catch (CommandLine.ParameterException e) {
         String path = e.getCommandLine().getCommandSpec().qualifiedName(" ");
         int space = path.indexOf(' ');
         path = space < 0 ? "-" : path.substring(space + 1);
         java.io.StringWriter suggestions = new java.io.StringWriter();
         CommandLine.UnmatchedArgumentException.printSuggestions(e, new java.io.PrintWriter(suggestions));
         return "error " + escape(e.getMessage()) + "  @  " + path + "  ?  " + escape(suggestions.toString().strip());
      }
   }

   static String escape(String s) {
      return s.replace("\n", "\\n").replace("\r", "\\r");
   }

   static String value(ArgSpec a) {
      Object v = a.getValue();
      if (v instanceof Object[] array) {
         return java.util.Arrays.toString(array);
      }
      return String.valueOf(v);
   }

   static void root(Set<List<String>> cases) {
      cases.add(List.of());
      cases.add(List.of("frob"));
      cases.add(List.of("frob", "x", "--y"));
      cases.add(List.of("--frob"));
      cases.add(List.of("--frob", "x"));
      cases.add(List.of("-x"));
      cases.add(List.of("--"));
      cases.add(List.of("--", "producer"));
      cases.add(List.of(""));
      cases.add(List.of("help"));
      cases.add(List.of("help", "frob"));
      cases.add(List.of("help", "--frob"));
      cases.add(List.of("help", "queue", "stat"));
      // Words like a group's commands and options, for the suggestions.
      for (String g : new String[] {"queue", "address", "data", "perf", "check", "user", "activation"}) {
         for (String w : new String[] {"stt", "stat", "creat", "crate", "Delete", "pruge", "lst", "lsit", "s", "st",
            "xyz", "--crate", "--st", "--u", "--user", "--usr", "-x", "imp", "exp", "prnt", "print", "clent", "clinet",
            "node", "nod", "qeue", "ad", "up", "updat", "remov", "rset", "reset", "a b", "Stat", "STAT", "ex", "e"}) {
            cases.add(List.of(g, w));
            cases.add(List.of(g, w, "x"));
         }
      }
      for (String c : new String[] {"run", "stop", "kill", "user", "perf-journal", "activation", "lock", "properties",
         "create", "upgrade", "data", "connect", "disconnect", "shell", "Producer", "PRODUCER", "help"}) {
         cases.add(List.of(c));
         cases.add(List.of(c, "--frob"));
      }
   }

   static void walk(Set<List<String>> cases, CommandLine line, List<String> path) {
      CommandSpec spec = line.getCommandSpec();
      if (!path.isEmpty()) {
         command(cases, spec, path);
      }
      for (Map.Entry<String, CommandLine> e : line.getSubcommands().entrySet()) {
         List<String> sub = new ArrayList<>(path);
         sub.add(e.getKey());
         walk(cases, e.getValue(), sub);
      }
   }

   static List<String> with(List<String> path, String... more) {
      List<String> l = new ArrayList<>(path);
      l.addAll(List.of(more));
      return l;
   }

   static void command(Set<List<String>> cases, CommandSpec spec, List<String> path) {
      cases.add(with(path));
      cases.add(with(path, "--frob"));
      cases.add(with(path, "--frob", "x"));
      cases.add(with(path, "--frob", "--bar"));
      cases.add(with(path, "--frob=x"));
      cases.add(with(path, "-f"));
      cases.add(with(path, "x"));
      cases.add(with(path, "x", "y"));
      cases.add(with(path, "x", "--frob"));
      cases.add(with(path, "--"));
      cases.add(with(path, "--", "x"));
      cases.add(with(path, "--", "--frob"));
      cases.add(with(path, ""));
      cases.add(with(path, "-"));
      cases.add(with(path, "-5"));
      cases.add(with(path, "-1.5"));
      cases.add(with(path, "-0x1F"));
      cases.add(with(path, "-e"));
      cases.add(with(path, "--frob", "-"));
      if (!spec.subcommands().isEmpty()) {
         cases.add(with(path, "frob"));
         cases.add(with(path, "frob", "x"));
         String first = spec.subcommands().keySet().iterator().next();
         cases.add(with(path, first.toUpperCase()));
      }
      List<OptionSpec> options = spec.options();
      // Another option of the command, to follow one that takes a value.
      String other = null;
      String flag = null;
      for (OptionSpec o : options) {
         if (o.arity().max() == 0 && flag == null) {
            flag = o.longestName();
         }
      }
      for (OptionSpec o : options) {
         String name = o.longestName();
         if (other == null || other.equals(name)) {
            for (OptionSpec x : options) {
               if (!x.longestName().equals(name)) {
                  other = x.longestName();
                  break;
               }
            }
         }
         for (String n : o.names()) {
            if (!n.equals(name)) {
               cases.add(with(path, n));
               cases.add(with(path, n, name));
               cases.add(with(path, name, n));
               cases.add(with(path, n + "=false"));
            }
         }
         cases.add(with(path, name.toUpperCase()));
         if (o.arity().max() == 0) {
            cases.add(with(path, name));
            cases.add(with(path, name, name));
            for (String v : new String[] {"true", "false", "TRUE", "False", "x", "", "1", "yes"}) {
               cases.add(with(path, name + "=" + v));
            }
            cases.add(with(path, name, "true"));
            cases.add(with(path, name, "false"));
         } else {
            cases.add(with(path, name));
            cases.add(with(path, name, "v"));
            cases.add(with(path, name + "=v"));
            cases.add(with(path, name + "="));
            cases.add(with(path, name, ""));
            cases.add(with(path, name + "=a=b"));
            cases.add(with(path, name, "v", name, "w"));
            if (other != null) {
               cases.add(with(path, name, other));
               cases.add(with(path, name, other + "=1"));
            }
            if (flag != null && !flag.equals(name)) {
               cases.add(with(path, name, flag));
            }
            cases.add(with(path, name, "--frob"));
            cases.add(with(path, name, "-v"));
            cases.add(with(path, name, "--"));
            Class<?> t = o.typeInfo().isMultiValue() ? o.typeInfo().getAuxiliaryTypes()[0] : o.type();
            if (t.isEnum()) {
               for (Object c : t.getEnumConstants()) {
                  String constant = ((Enum<?>) c).name();
                  cases.add(with(path, name, constant));
                  cases.add(with(path, name, constant.toLowerCase()));
               }
            }
            if (t != String.class) {
               cases.add(with(path, name, "1", name, "w"));
               cases.add(with(path, name, "w", name, "1"));
               cases.add(with(path, name, "1", name, "2"));
               for (String v : new String[] {"12", "-5", "+7", "0", "007", "0x10", "1.5", "1e3", "2147483648",
                  "9223372036854775808", " 3", "3 ", "1_000", "abc", "true", "NaN", "Infinity", "-0.0", "CORE", "core",
                  "AMQP", "OPENWIRE", "SECONDS", "seconds"}) {
                  cases.add(with(path, name, v));
               }
               cases.add(with(path, name + "=12"));
               cases.add(with(path, name + "=abc"));
            }
            if (o.typeInfo().isMultiValue()) {
               cases.add(with(path, name, "a", name, "b"));
               cases.add(with(path, name, "a,b"));
            }
         }
      }
      for (PositionalParamSpec p : spec.positionalParameters()) {
         cases.add(with(path, "a"));
         cases.add(with(path, "a", "b", "c"));
         cases.add(with(path, "-a"));
         cases.add(with(path, "--", "-a"));
         if (flag != null) {
            cases.add(with(path, "a", flag));
            cases.add(with(path, flag, "a"));
         }
         if (other != null) {
            cases.add(with(path, "a", other));
         }
      }
   }
}
