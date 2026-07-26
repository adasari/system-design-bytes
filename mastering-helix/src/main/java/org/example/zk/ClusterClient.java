package org.example.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class ClusterClient implements Watcher {

  @Override
  public void process(WatchedEvent event) {
    System.out.printf("\nEvent Received: %s\n", event.toString());
  }
}
