package org.example.helix;

import org.apache.helix.participant.statemachine.StateModelFactory;


public class DemoClusterStateModelFactory extends StateModelFactory<HelixCluster.DemoStateModel> {
  private final String instanceName;

  public DemoClusterStateModelFactory(String instanceName) {
    this.instanceName = instanceName;
  }

  @Override
  public HelixCluster.DemoStateModel createNewStateModel(String resourceName, String partitionName) {
    return new HelixCluster.DemoStateModel(instanceName, resourceName, partitionName);
  }
}
