package org.example.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private String instanceName;

  public OnlineOfflineStateModelFactory(String instanceName) {
    this.instanceName = instanceName;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel(instanceName);
    return stateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFINE")
  public static class OnlineOfflineStateModel extends StateModel {
    private static final Logger logger = LoggerFactory.getLogger(OnlineOfflineStateModel.class);

    private String instanceName;

    public OnlineOfflineStateModel(String instanceName) {
      this.instanceName = instanceName;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message,
        NotificationContext context) {
      logger.info("**** OnlineOfflineStateModel.onBecomeOnlineFromOffline() ** {}: {}", instanceName, message.toString());
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message,
        NotificationContext context) {
      logger.info("**** OnlineOfflineStateModel.onBecomeOfflineFromOnline() ** {}: {}", instanceName, message.toString());
    }
  }
}
