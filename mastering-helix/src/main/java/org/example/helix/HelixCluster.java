package org.example.helix;

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.Transition;


public class HelixCluster {

  private static final String ZK_ADDRESS = "localhost:2181";
  private static final String CLUSTER_NAME = "demo-cluster2";
  private static final String RESOURCE_NAME = "demo-resource";
  private static final int NUM_PARTITIONS = 5;
  private static final int NUM_REPLICAS = 2;

  public static void main(String[] args) throws Exception{
    // Step 1: Setup cluster
    setupCluster();

    // Step 2: Start controller
    Thread controllerThread = startController();

    // Step 3: Start participants
    List<Thread> participantThreads = startParticipants(3);

    // Let the cluster run for a bit
    System.out.println("\nCluster is running. Press Ctrl+C to stop.\n");
    Thread.sleep(30000);

    // Cleanup
    System.out.println("Shutting down...");
    controllerThread.interrupt();
    participantThreads.forEach(Thread::interrupt);

  }

  private static void setupCluster() {
    System.out.println("Step 1: Setting up cluster '" + CLUSTER_NAME + "'");

    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);
    // admin.dropCluster(CLUSTER_NAME);

    // Create cluster if it doesn't exist
    if (!admin.getClusters().contains(CLUSTER_NAME)) {
      admin.addCluster(CLUSTER_NAME);
      System.out.println("Cluster created");
    } else {
      System.out.println("Cluster already exists");
    }

    // Add state model definition (using LeaderStandBy model)
    StateModelDefinition standbyLeader = new StateModelDefinition.Builder("LeaderStandBy")
        .initialState("OFFLINE")
        .addState("STANDBY", 2)
        .addState("LEADER", 1)
        .addState("OFFLINE")
        .addState("DROPPED")
        .addTransition("OFFLINE", "STANDBY", 2)
        .addTransition("STANDBY", "OFFLINE", 3)
        .addTransition("STANDBY", "LEADER", 1)
        .addTransition("LEADER", "STANDBY", 2)
        .addTransition("OFFLINE", "DROPPED")
        .dynamicUpperBound("LEADER", "1")
        .dynamicUpperBound("STANDBY", "R")
        .build();

    admin.addStateModelDef(CLUSTER_NAME, "LeaderStandBy", standbyLeader);
    System.out.println("State model definition added");

    // Add resource
    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, "LeaderStandBy",
        IdealState.RebalanceMode.FULL_AUTO.toString());

    // Rebalance the resource
    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
    System.out.println("  Resource '" + RESOURCE_NAME + "' added with " +
        NUM_PARTITIONS + " partitions\n");

    admin.close();
  }

  private static Thread startController() {
    System.out.println("Step 2: Starting controller");

    Thread thread = new Thread(() -> {
      try {
        HelixManager controller = HelixManagerFactory.getZKHelixManager(
            CLUSTER_NAME,
            "controller",
            InstanceType.CONTROLLER,
            ZK_ADDRESS
        );

        controller.connect();
        System.out.println("Controller started and connected");

        // Keep controller running
        Thread.currentThread().join();

        controller.disconnect();
      } catch (Exception e) {
        if (!(e instanceof InterruptedException)) {
          e.printStackTrace();
        }
      }
    });

    thread.start();

    try {
      Thread.sleep(2000); // Give controller time to start
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return thread;
  }


  private static List<Thread> startParticipants(int count) throws Exception {
    System.out.println("Step 3: Starting " + count + " participants");

    List<Thread> threads = new ArrayList<>();
    HelixAdmin admin = new ZKHelixAdmin(ZK_ADDRESS);

    for (int i = 0; i < count; i++) {
      String instanceName = "participant_" + i;

      // Add instance to cluster
      if (!admin.getInstancesInCluster(CLUSTER_NAME).contains(instanceName)) {
        admin.addInstance(CLUSTER_NAME,
            new org.apache.helix.model.InstanceConfig(instanceName));
      }

      Thread thread = startParticipant(instanceName);
      threads.add(thread);

      System.out.println("Participant '" + instanceName + "' started");
      Thread.sleep(1000);
    }

    admin.close();
    System.out.println();
    return threads;
  }

  /**
   * Starts a single participant node
   */
  private static Thread startParticipant(String instanceName) {
    Thread thread = new Thread(() -> {
      try {
        HelixManager participant = HelixManagerFactory.getZKHelixManager(
            CLUSTER_NAME,
            instanceName,
            InstanceType.PARTICIPANT,
            ZK_ADDRESS
        );

        // Register state model factory
        StateMachineEngine stateMachine = participant.getStateMachineEngine();
        stateMachine.registerStateModelFactory("LeaderStandBy",
            new DemoStateModelFactory(instanceName));

        participant.connect();

        // Keep participant running
        Thread.currentThread().join();

        participant.disconnect();
      } catch (Exception e) {
        if (!(e instanceof InterruptedException)) {
          e.printStackTrace();
        }
      }
    });

    thread.start();
    return thread;
  }

  /**
   * State Model Factory - creates state models for partitions
   */
  static class DemoStateModelFactory extends StateModelFactory<DemoStateModel> {
    private final String instanceName;

    public DemoStateModelFactory(String instanceName) {
      this.instanceName = instanceName;
    }

    @Override
    public DemoStateModel createNewStateModel(String resourceName, String partitionName) {
      return new DemoStateModel(instanceName, resourceName, partitionName);
    }
  }

  /**
   * State Model - handles state transitions for partitions
   */
  static class DemoStateModel extends StateModel {
    private final String instanceName;
    private final String resourceName;
    private final String partitionName;

    public DemoStateModel(String instanceName, String resourceName, String partitionName) {
      this.instanceName = instanceName;
      this.resourceName = resourceName;
      this.partitionName = partitionName;
    }

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline() {
      System.out.println(String.format("[%s] %s_%s: OFFLINE → STANDBY",
          instanceName, resourceName, partitionName));
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby() {
      System.out.println(String.format("[%s] %s_%s: STANDBY → LEADER ⭐",
          instanceName, resourceName, partitionName));
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader() {
      System.out.println(String.format("[%s] %s_%s: LEADER → STANDBY",
          instanceName, resourceName, partitionName));
    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby() {
      System.out.println(String.format("[%s] %s_%s: STANDBY → OFFLINE",
          instanceName, resourceName, partitionName));
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline() {
      System.out.println(String.format("[%s] %s_%s: OFFLINE → DROPPED",
          instanceName, resourceName, partitionName));
    }
  }

}
