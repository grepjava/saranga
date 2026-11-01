// The GSSAPI suite's directory: ApacheDS, as Artemis's SaslKrb5LDAPSecurityTest
// runs it, taking GSSAPI binds as ldap/localhost@EXAMPLE.COM. Its keys come
// from its own entry for that principal (KeyDerivationInterceptor derives
// them from the password), which Kdc.java gives the same password, so the
// KDC's tickets for it decrypt. The clients' entries and a group follow
// SaslKrb5LDAPSecurityTest.ldif, for principals of their own (logins are
// cached by user across realms). Writes "ready" into its directory, and
// serves until killed.
//
//   java -cp '<directory-classpath.txt's jars>' Directory.java dir port [ldaps-port keystore.p12]
//
// Run by tests/interop/gssapi/gssapi.sh.

import java.io.File;
import java.nio.file.Files;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;

@CreateDS(name = "Example",
   partitions = {@CreatePartition(name = "example", suffix = "dc=example,dc=com",
      contextEntry = @ContextEntry(entryLdif = "dn: dc=example,dc=com\ndc: example\nobjectClass: top\nobjectClass: domain\n\n"),
      indexes = {@CreateIndex(attribute = "objectClass"), @CreateIndex(attribute = "dc"), @CreateIndex(attribute = "ou")})},
   additionalInterceptors = {KeyDerivationInterceptor.class})
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)},
   saslHost = "localhost",
   saslPrincipal = "ldap/localhost@EXAMPLE.COM",
   saslMechanisms = {@SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class)})
public class Directory {
   public static final String LDAP_PASSWORD = "ldap-service-secret";

   static String principal(String uid, String password) {
      return "dn: uid=" + uid.replace("/", "_") + ",ou=users,dc=example,dc=com\n"
         + "objectClass: top\nobjectClass: person\nobjectClass: inetOrgPerson\n"
         + "objectClass: krb5principal\nobjectClass: krb5kdcentry\n"
         + "cn: " + uid + "\nsn: " + uid + "\nuid: " + uid.replace("/", "_") + "\n"
         + "userPassword: " + password + "\n"
         + "krb5PrincipalName: " + uid + "@EXAMPLE.COM\n"
         + "krb5KeyVersionNumber: 1\n\n";
   }

   public static void main(String[] args) throws Exception {
      File dir = new File(args[0]);
      int port = Integer.parseInt(args[1]);
      dir.mkdirs();
      DirectoryService ds = DSAnnotationProcessor.createDS(Directory.class.getAnnotation(CreateDS.class));
      DSAnnotationProcessor.injectEntries(ds, "dn: ou=users,dc=example,dc=com\nobjectClass: organizationalUnit\nobjectClass: top\nou: users\n\n"
         + principal("ldap/localhost", LDAP_PASSWORD)
         // The broker binds as amqp/localhost, which must have an entry.
         + principal("amqp/localhost", "unused-amqp-secret")
         + principal("ldapclient", "unused-ldapclient-secret")
         + principal("ldapother", "unused-ldapother-secret")
         + principal("ldapsclient", "unused-ldapsclient-secret")
         + "dn: cn=admins,ou=system\ncn: admins\nmember: uid=ldapclient,ou=users,dc=example,dc=com\n"
         + "member: uid=ldapsclient,ou=users,dc=example,dc=com\n"
         + "objectClass: groupOfNames\nobjectClass: top\n\n");
      LdapServer ldap = ServerAnnotationProcessor.instantiateLdapServer(Directory.class.getAnnotation(CreateLdapServer.class), ds);
      TcpTransport plain = new TcpTransport("localhost", port);
      if (args.length > 3) {
         // LDAPS on another port, with the keystore given.
         TcpTransport ssl = new TcpTransport("localhost", Integer.parseInt(args[2]));
         ssl.setEnableSSL(true);
         ldap.setTransports(plain, ssl);
         ldap.setKeystoreFile(args[3]);
         ldap.setCertificatePassword("changeit");
      } else {
         ldap.setTransports(plain);
      }
      ldap.setSearchBaseDn("ou=users,dc=example,dc=com");
      if (!ldap.isStarted()) {
         ldap.start();
      }
      Files.writeString(new File(dir, "ready").toPath(), "ready\n");
      Thread.currentThread().join();
   }
}
