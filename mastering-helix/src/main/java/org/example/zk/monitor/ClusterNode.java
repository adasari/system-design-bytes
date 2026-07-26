package org.example.zk.monitor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClusterNode {
  private static final String CLUSTER_ROOT = "/cluster";

  private String nodeId;
  private ZooKeeper _zooKeeper;
  AtomicBoolean running = new AtomicBoolean(false);

  public ClusterNode(String nodeId) throws Exception {
    this.nodeId = nodeId;
    CountDownLatch latch = new CountDownLatch(1);
    _zooKeeper = new ZooKeeper("localhost:2181", 5000, event -> {
      if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
        latch.countDown();
      }
    });
//    init();
    register();
    running.set(true);
  }

  private void init() throws Exception {
    Stat stat = _zooKeeper.exists(CLUSTER_ROOT, false);
    if (stat == null) {
      _zooKeeper.create(
          CLUSTER_ROOT,
          new byte[0],
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT
      );
    }
  }

  private void register() throws Exception {
    System.out.println("Registering node " + nodeId);
    String path = CLUSTER_ROOT + "/" + nodeId;
    _zooKeeper.create(
        path,
        new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL
    );
    System.out.println("Node " + nodeId + " registered at " + path);
  }

  public void stop() throws Exception {
    System.out.println("Shutdown node " + nodeId);
    running.set(false);
    if (_zooKeeper != null) {
      _zooKeeper.close();
    }
  }
}
