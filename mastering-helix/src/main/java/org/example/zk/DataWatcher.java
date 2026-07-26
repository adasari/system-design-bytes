package org.example.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;


public class DataWatcher implements Watcher, Runnable {
  private static final String zkHost = "localhost:2181";

  private String zkDataPath = null;
  private ZooKeeper zk;

  public DataWatcher(String zkDataPath)
      throws InterruptedException, KeeperException {
    this.zkDataPath = zkDataPath;
    try {
      zk = new ZooKeeper(zkHost, 2000, this);
      if (zk.exists(zkDataPath, this) == null) {
          // create znode
          System.out.println("Creating znode " + zkDataPath);
          zk.create(zkDataPath, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    } catch (Exception ex) {
      ex.printStackTrace();
      System.out.println("Creating znode " + zkDataPath);
      zk.create(zkDataPath, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  public void show() {
    show(this.zkDataPath);
  }

  public void show(String zkDataPath) {
    try {
      byte[] data = zk.getData(zkDataPath, this, null);
      String dataStr = new String(data);
      System.out.printf("\nCurrent Data of %s : %s\n", zkDataPath, dataStr);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
      DataWatcher dataWatcher = new DataWatcher("/MyConfig");
      dataWatcher.show();
      dataWatcher.run();
  }

  @Override
  public void run() {
    try {
      synchronized (this) {
        while (true) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    System.out.printf("Event received: %s\n", watchedEvent.toString());
    if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
      show();
    }
  }
}
