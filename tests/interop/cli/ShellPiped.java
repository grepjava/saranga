/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

import org.apache.activemq.artemis.cli.Shell;

/**
 * The Artemis shell with its input piped (`Shell.runShell` with an input
 * stream, as Artemis's ShellTest runs it): the lines of standard input run
 * one after another. The instance's etc/history-file must not be empty, or
 * the shell first asks whether to keep a history.
 */
public class ShellPiped {
   public static void main(String[] args) {
      Shell.runShell(false, null, System.in);
   }
}
