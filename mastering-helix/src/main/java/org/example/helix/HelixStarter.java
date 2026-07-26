package org.example.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixStarter {

  public static final Logger logger = LoggerFactory.getLogger(HelixStarter.class);
  private static final String CLUSTER_NAME = "demo-cluster";
  private static final String ZK_ADDRESS = "localhost:2181";

  private static final String ONLINE_OFFLINE_STATE_MODEL = "OnlineOffline";
  private static final String RESOURCE_NAME = "DemoResource";

  public static void main(String[] args) {
    if (args.length == 0) {
      logger.info("Please provide a mode: setup | controller | participant");
      return;
    }

    System.out.println("Args: " + String.join(",", args));

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
    String type = args[0];
    HelixManager manager = null;
    switch (type.toLowerCase()) {
      case "setup":
        setupCluster();
        System.exit(0);
      case "controller":
        manager = HelixManagerFactory.getZKHelixManager(
            CLUSTER_NAME,
            "controller_" + System.currentTimeMillis(),
            InstanceType.CONTROLLER,
            ZK_ADDRESS
        );

        break;
      case "participant":

        String instanceName = System.getProperty("app.instance", "participant_" + System.currentTimeMillis());

          // Add instance to cluster
        if (!admin.getInstancesInCluster(CLUSTER_NAME).contains(instanceName)) {
          admin.addInstance(CLUSTER_NAME,
              new org.apache.helix.model.InstanceConfig(instanceName));
        }

        manager = HelixManagerFactory.getZKHelixManager(
            CLUSTER_NAME,
            instanceName,
            InstanceType.PARTICIPANT,
            ZK_ADDRESS
        );
        StateMachineEngine stateMachine = manager.getStateMachineEngine();
        stateMachine.registerStateModelFactory(ONLINE_OFFLINE_STATE_MODEL,
            new OnlineOfflineStateModelFactory(instanceName));
        break;
      case "resource":
        CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(ONLINE_OFFLINE_STATE_MODEL);
        customModeIdealStateBuilder
            .setStateModel(ONLINE_OFFLINE_STATE_MODEL)
            .setNumPartitions(0).setNumReplica(2).setMaxPartitionsPerNode(1);
        IdealState idealState = customModeIdealStateBuilder.build();
        idealState.setInstanceGroupTag(RESOURCE_NAME);

        admin.addResource(CLUSTER_NAME, RESOURCE_NAME, idealState);
        logger.info("Resource '" + RESOURCE_NAME + "' added to cluster '" + CLUSTER_NAME);
        System.exit(0);
      default:
        logger.warn("Unknown mode: " + type);
        System.exit(0);
    }

    try {
      manager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets up the Helix cluster with necessary configurations
   */
  private static void setupCluster() {
    logger.info("Setting up cluster '" + CLUSTER_NAME + "'");

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
    // admin.dropCluster(CLUSTER_NAME);

    // Create cluster if it doesn't exist
    if (admin.getClusters().contains(CLUSTER_NAME)) {
      admin.addCluster(CLUSTER_NAME, true);
      logger.info("Cluster has been created");
    } else {
      admin.addCluster(CLUSTER_NAME, true);
      logger.info("Cluster created");
    }

    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(ONLINE_OFFLINE_STATE_MODEL);
    // Set the initial state when the node starts
    builder.initialState("OFFLINE");

    builder.addState("ONLINE");
    builder.addState("OFFLINE");
    builder.addState("DROPPED");

    // Add transitions between the states.
    builder.addTransition("OFFLINE", "ONLINE", 1);
    builder.addTransition("ONLINE", "OFFLINE", 2);
    builder.addTransition("OFFLINE", "DROPPED", 3);
    builder.addTransition("ONLINE", "DROPPED", 3);

    builder.dynamicUpperBound("ONLINE", "R");
    StateModelDefinition statemodelDefinition = builder.build();

    admin.addStateModelDef(CLUSTER_NAME, ONLINE_OFFLINE_STATE_MODEL, statemodelDefinition);
    logger.info("State model definition added");

    admin.close();
  }

}
