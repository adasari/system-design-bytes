package org.example.zk;

import java.util.UUID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;


public class DataUpdater implements Watcher {

  private static final String zkHost = "localhost:2181";
  private String zkDataPath = null;

  private ZooKeeper zk;

  public DataUpdater(String zkDataPath) {
    this.zkDataPath = zkDataPath;
    try {
      zk = new ZooKeeper(zkHost, 2000, this);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public void run() throws Exception {
    while (true) {
      String uuid = UUID.randomUUID().toString();
      zk.setData(this.zkDataPath, uuid.getBytes(), -1);

      try {
        Thread.sleep(5000);
      } catch (Exception ex) {
          Thread.currentThread().interrupt();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    DataUpdater dataUpdater = new DataUpdater("/MyConfig");
    dataUpdater.run();
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    System.out.printf("\nEvent Received: %s\n", watchedEvent.toString());
  }
}
