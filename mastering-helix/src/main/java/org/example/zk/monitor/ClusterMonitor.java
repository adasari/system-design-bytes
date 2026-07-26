package org.example.zk.monitor;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ClusterMonitor {
  private AtomicBoolean running = new AtomicBoolean(false);
  private String clusterRoot;
  private ZooKeeper _zooKeeper;

  public ClusterMonitor(String clusterRoot) throws Exception {
    this.clusterRoot = clusterRoot;
    CountDownLatch latch = new CountDownLatch(1);
    _zooKeeper = new ZooKeeper(
        "localhost:2181",
        5000,
        event -> {
          if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
            latch.countDown();
          }

          if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            refresh();
          }
        }
    );

    latch.await();
//    init();
    running.set(true);
    refresh();
  }

  private void init() throws Exception {
    Stat stat = _zooKeeper.exists(clusterRoot, false);
    if (stat == null) {
      _zooKeeper.create(
          clusterRoot,
          new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT
      );
    }
  }

  private void refresh() {
    try {
      List<String> nodes = _zooKeeper.getChildren(clusterRoot, true);
      System.out.println("Cluster nodes: " + nodes);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void stop() throws Exception {
    running.set(false);
    if (_zooKeeper != null) {
      _zooKeeper.close();
    }
  }
}
