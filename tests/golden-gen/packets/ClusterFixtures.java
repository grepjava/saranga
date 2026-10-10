// Fixtures for crates/saranga-core-protocol/src/packets/{cluster,cluster_types}.rs.

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationDownstreamConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationPolicy;
import org.apache.activemq.artemis.core.config.federation.FederationPolicySet;
import org.apache.activemq.artemis.core.config.federation.FederationQueuePolicyConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationTransformerConfiguration;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;
import org.apache.activemq.artemis.core.server.cluster.quorum.BooleanVote;
import org.apache.activemq.artemis.core.server.cluster.quorum.ServerConnectVote;
import org.apache.activemq.artemis.core.server.cluster.quorum.Vote;

public class ClusterFixtures {
   private static final String NETTY = "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";

   static TransportConfiguration connector(String name, String host, int port) {
      Map<String, Object> params = new HashMap<>();
      params.put("host", host);
      params.put("port", port);
      params.put("sslEnabled", false);
      return new TransportConfiguration(NETTY, params, name);
   }

   /** Same layout as the package-private ColocatedActivation.RequestBackupVote. */
   static Vote<Object> requestBackupVote(int backupsSize, String nodeID, boolean available) {
      return new Vote<>() {
         @Override
         public void encode(ActiveMQBuffer buff) {
            buff.writeInt(backupsSize);
            buff.writeNullableString(nodeID);
            buff.writeBoolean(available);
         }

         @Override
         public void decode(ActiveMQBuffer buff) {
         }

         @Override
         public boolean isRequestServerVote() {
            return true;
         }

         @Override
         public Object getVote() {
            return null;
         }
      };
   }

   public static void main(String[] args) throws Exception {
      Fixtures f = new Fixtures(args);
      TransportConfiguration primary = connector("primary", "broker-1", 61616);
      TransportConfiguration backup = connector("backup", "broker-2", 61617);
      String node = "8f2c4b1e-0a9d-11ef-9a3c-0242ac120002";

      // Topology
      f.emit("SubscribeClusterTopologyUpdatesMessage", "s", 136, new SubscribeClusterTopologyUpdatesMessage(true));
      f.emit("SubscribeClusterTopologyUpdatesMessageV2", "s", 136, new SubscribeClusterTopologyUpdatesMessageV2(false, 136));

      f.emit("ClusterTopologyChangeMessage", "c", 136,
         new ClusterTopologyChangeMessage(node, new Pair<>(primary, backup), true));
      f.emit("ClusterTopologyChangeMessage~nobackup", "c", 136,
         new ClusterTopologyChangeMessage(node, new Pair<>(primary, null), false));
      f.emit("ClusterTopologyChangeMessage~exit", "c", 136, new ClusterTopologyChangeMessage(node));

      f.emit("ClusterTopologyChangeMessageV2", "c", 136,
         new ClusterTopologyChangeMessage_V2(1_700_000_000_123L, node, "group-a", new Pair<>(primary, backup), true));
      f.emit("ClusterTopologyChangeMessageV2~nobackup", "c", 136,
         new ClusterTopologyChangeMessage_V2(5L, node, null, new Pair<>(primary, null), false));
      f.emit("ClusterTopologyChangeMessageV2~exit", "c", 136, new ClusterTopologyChangeMessage_V2(6L, node));

      f.emit("ClusterTopologyChangeMessageV3", "c", 136, new ClusterTopologyChangeMessage_V3(
         7L, node, "group-a", "scale-group", new Pair<>(primary, backup), true));
      f.emit("ClusterTopologyChangeMessageV3~nobackup", "c", 136, new ClusterTopologyChangeMessage_V3(
         8L, node, null, null, new Pair<>(primary, null), false));
      f.emit("ClusterTopologyChangeMessageV3~noprimary", "c", 136, new ClusterTopologyChangeMessage_V3(
         9L, node, "group-b", null, new Pair<>(null, backup), true));

      f.emit("ClusterTopologyChangeMessageV4", "c", 136, new ClusterTopologyChangeMessage_V4(
         10L, node, "group-a", "scale-group", new Pair<>(primary, backup), true, 136));
      f.emit("ClusterTopologyChangeMessageV4~nobackup", "c", 136, new ClusterTopologyChangeMessage_V4(
         11L, node, null, null, new Pair<>(primary, null), false, 135));

      f.emit("NodeAnnounceMessage", "s", 136, new NodeAnnounceMessage(
         12L, node, "group-a", "scale-group", false, primary, backup));
      f.emit("NodeAnnounceMessage~backup", "s", 136, new NodeAnnounceMessage(
         13L, node, null, null, true, backup, null));

      // Cluster connections
      f.emit("ClusterConnectMessage", "s", 136, new ClusterConnectMessage("cluster-admin", "cluster-secret"));
      f.emit("ClusterConnectReplyMessage", "c", 136, new ClusterConnectReplyMessage(true));
      f.emit("ClusterConnectReplyMessage~denied", "c", 136, new ClusterConnectReplyMessage(false));

      // HA
      f.emit("BackupRegistrationMessage", "s", 136,
         new BackupRegistrationMessage(backup, "cluster-admin", "cluster-secret", true));
      f.emit("BackupReplicationStartFailedMessage", "c", 136, new BackupReplicationStartFailedMessage(
         BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
      f.emit("BackupRequestMessage", "s", 136, new BackupRequestMessage(
         2, "data/journal", "data/bindings", "data/large-messages", "data/paging"));
      f.emit("BackupRequestMessage~nodeid", "s", 136, new BackupRequestMessage(1, SimpleString.of(node)));
      f.emit("BackupResponseMessage", "c", 136, new BackupResponseMessage(true));

      SimpleString primaryVote = SimpleString.of("PrimaryFailoverQuorumVote");
      f.emit("QuorumVoteMessage", "s", 136,
         new QuorumVoteMessage(primaryVote, new ServerConnectVote(node, true, null)));
      f.emit("QuorumVoteMessage~oldname", "s", 136,
         new QuorumVoteMessage(SimpleString.of("LiveFailoverQuorumVote"), new ServerConnectVote(node, false, null)));
      f.emit("QuorumVoteMessage~requestbackup", "s", 136,
         new QuorumVoteMessage(SimpleString.of("RequestBackupQuorumVote"), requestBackupVote(-1, null, false)));
      f.emit("QuorumVoteMessage~unknown", "s", 136,
         new QuorumVoteMessage(SimpleString.of("SomeOtherVote"), new BooleanVote(true)));
      f.emit("QuorumVoteReplyMessage", "c", 136,
         new QuorumVoteReplyMessage(primaryVote, new ServerConnectVote(node, true, primary.toString())));
      f.emit("QuorumVoteReplyMessage~requestbackup", "c", 136,
         new QuorumVoteReplyMessage(SimpleString.of("RequestBackupQuorumVote"), requestBackupVote(3, node, true)));

      f.emit("ScaleDownAnnounceMessage", "s", 136,
         new ScaleDownAnnounceMessage(SimpleString.of("target-node"), SimpleString.of(node)));

      // Connect / failover check
      f.emit("ConnectMessage", "s", 136, new ConnectMessage(node, 136));
      f.emit("ConnectMessage~empty", "s", 136, new ConnectMessage(null, 0));
      f.emit("ConnectMessage~plain", "s", 136, ConnectMessage.withPlainCredentials(node, 136, "user", "pässword"));
      f.emit("ConnectResponseMessage", "c", 136, new ConnectResponseMessage(true, 136));
      f.emit("ConnectResponseMessage~noversion", "c", 136, new ConnectResponseMessage(false, 0));

      // Federation
      FederationDownstreamConnectMessage fed = new FederationDownstreamConnectMessage();
      fed.setName("federation-1");
      fed.setCredentials(new FederationConfiguration.Credentials().setUser("fed-user").setPassword("fed-pass"));

      Map<String, FederationPolicy<?>> policies = new LinkedHashMap<>();
      FederationAddressPolicyConfiguration address = new FederationAddressPolicyConfiguration()
         .setName("address-policy").setAutoDelete(true).setAutoDeleteDelay(60_000L).setMaxHops(2)
         .setTransformerRef("transformer-1").setEnableDivertBindings(true)
         .addInclude(new FederationAddressPolicyConfiguration.Matcher().setAddressMatch("orders.#"))
         .addExclude(new FederationAddressPolicyConfiguration.Matcher().setAddressMatch("orders.internal"));
      FederationQueuePolicyConfiguration queue = new FederationQueuePolicyConfiguration()
         .setName("queue-policy").setIncludeFederated(true).setPriorityAdjustment(-1)
         .addInclude(new FederationQueuePolicyConfiguration.Matcher().setAddressMatch("jobs.#").setQueueMatch("worker.*"))
         .addExclude(new FederationQueuePolicyConfiguration.Matcher().setQueueMatch("dlq"));
      FederationPolicySet set = new FederationPolicySet().setName("policy-set").addPolicyRef("address-policy");
      policies.put(address.getName(), address);
      policies.put(queue.getName(), queue);
      policies.put(set.getName(), set);
      fed.setFederationPolicyMap(policies);

      Map<String, FederationTransformerConfiguration> transformers = new LinkedHashMap<>();
      TransformerConfiguration tc = new TransformerConfiguration("com.example.Transformer");
      tc.getProperties().put("mode", "upper");
      transformers.put("transformer-1", new FederationTransformerConfiguration("transformer-1", tc));
      fed.setTransformerConfigurationMap(transformers);

      FederationDownstreamConfiguration downstream = new FederationDownstreamConfiguration();
      downstream.setName("downstream-1");
      downstream.addPolicyRef("policy-set");
      downstream.getConnectionConfiguration().setUsername("down-user").setPassword("down-pass")
         .setShareConnection(true).setPriorityAdjustment(-2).setRetryInterval(750).setRetryIntervalMultiplier(1.5)
         .setMaxRetryInterval(9_000).setReconnectAttempts(10);
      downstream.setUpstreamConfiguration(connector("upstream", "broker-3", 61618));
      fed.setStreamConfiguration(downstream);
      f.emit("FederationDownstreamConnectMessage", "s", 136, fed);

      FederationDownstreamConnectMessage minimal = new FederationDownstreamConnectMessage();
      minimal.setName("federation-2");
      FederationDownstreamConfiguration bare = new FederationDownstreamConfiguration();
      bare.setName("downstream-2");
      bare.setUpstreamConfiguration(connector("upstream", "broker-4", 61619));
      minimal.setStreamConfiguration(bare);
      f.emit("FederationDownstreamConnectMessage~minimal", "s", 136, minimal);

      f.done();
   }
}
