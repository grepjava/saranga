// Records a golden file of the configuration exported with broker
// properties applied: what Artemis's exportConfigAsProperties writes for a
// broker started from that broker.xml and those broker properties.
//
// `artemis properties` reads broker.xml alone; this reads it as that
// command does (Configurable.readConfiguration), then applies the broker
// properties as ActiveMQServerImpl does at start (parseProperties of the
// sources), then exports.
//
//   java -Xmx1g -cp "<artemis home>/lib/*" ExportWithProperties.java \
//     <instance> <case>.xml <case>.broker.properties <case>.properties
//
// The instance is an Artemis 2.57 instance (its etc/broker.xml replaced by
// the case's), so brokerInstance and configurationUrl are as
// `artemis properties` gives them.

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;

public class ExportWithProperties {
   public static void main(String[] args) throws Exception {
      File instance = new File(args[0]).getCanonicalFile();
      File brokerXml = new File(instance, "etc/broker.xml");
      Files.copy(new File(args[1]).toPath(), brokerXml.toPath(), StandardCopyOption.REPLACE_EXISTING);
      FileConfiguration configuration = new FileConfiguration();
      FileJMSConfiguration jms = new FileJMSConfiguration();
      FileDeploymentManager manager = new FileDeploymentManager(brokerXml.toURI().toASCIIString());
      manager.addDeployable(configuration).addDeployable(jms);
      manager.readConfiguration();
      configuration.setBrokerInstance(instance);
      configuration.parseProperties(new File(args[2]).getPath().replace('\\', '/'));
      configuration.exportAsProperties(new File(args[3]));
      System.out.println(configuration.getStatus());
   }
}
