// The GSSAPI suite's KDC: Apache Kerby's SimpleKdcServer for the realm
// EXAMPLE.COM over TCP, with the broker's service principals in one keytab,
// the clients' in another, and a user with a password. Writes krb5.conf and
// then "ready" into its directory, and serves until killed.
//
//   java -cp '<artemis>/lib/*:<kerby jars>' Kdc.java dir port
//
// Run by tests/interop/gssapi/gssapi.sh.

import java.io.File;
import java.nio.file.Files;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

public class Kdc {
   public static void main(String[] args) throws Exception {
      File dir = new File(args[0]);
      int port = Integer.parseInt(args[1]);
      dir.mkdirs();
      File conf = new File(dir, "krb5.conf");
      Files.writeString(conf.toPath(), """
            [libdefaults]
                default_realm = EXAMPLE.COM
                udp_preference_limit = 1
                dns_lookup_kdc = false
                dns_lookup_realm = false
                dns_canonicalize_hostname = false
                rdns = false

            [realms]
                EXAMPLE.COM = {
                    kdc = localhost:%d
                }

            [domain_realm]
                localhost = EXAMPLE.COM
                otherhost = EXAMPLE.COM
            """.formatted(port));

      SimpleKdcServer kdc = new SimpleKdcServer();
      kdc.setWorkDir(dir);
      kdc.setKdcRealm("EXAMPLE.COM");
      kdc.setKdcHost("localhost");
      kdc.setKdcTcpPort(port);
      kdc.setAllowUdp(false);
      kdc.init();
      kdc.start();
      kdc.createAndExportPrincipals(new File(dir, "service.keytab"), "amqp/localhost", "amqp/otherhost");
      kdc.createAndExportPrincipals(new File(dir, "client.keytab"), "client", "norole");
      kdc.createPrincipal("pwuser", "pw-secret");
      Files.writeString(new File(dir, "ready").toPath(), "ready\n");
      Thread.currentThread().join();
   }
}
