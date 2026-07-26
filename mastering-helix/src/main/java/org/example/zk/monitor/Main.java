package org.example.zk.monitor;

import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class Main {

  private static final String CLUSTER_ROOT = "/cluster";

  public static void main(String[] args) throws Exception {
    boolean created = false;
    // create cluster node
    ZooKeeper _zooKeeper = new ZooKeeper("localhost:2181", 5000, null);
    while (!created) {
      try {
        Stat stat = _zooKeeper.exists("/clusters", false);
        if (stat == null) {
          _zooKeeper.create("/clusters", new byte[0],
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        }
        created = true; // success
      } catch (KeeperException.ConnectionLossException e) {
        System.out.println("Connection lost, retrying...");
        Thread.sleep(1000); // backoff before retry
      } catch (KeeperException.NodeExistsException e) {
        created = true; // someone else created it
      } catch (KeeperException e) {
        e.printStackTrace();
        Thread.sleep(1000);
      }
    }

    ClusterMonitor monitor = new ClusterMonitor(CLUSTER_ROOT);
    List<Thread> threads = new ArrayList<>();
    int nodeCount = 10;
    for (int i = 0; i < nodeCount; i++) {
      final String nodeId = "Node-" + i;
      Thread nodeThread = new Thread(() -> {
        try {
          ClusterNode node = new ClusterNode(nodeId);
          // run node for 120 seconds
          Thread.sleep(120000);
          node.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads.add(nodeThread);
      nodeThread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    System.out.println("All nodes have stopped.");
    monitor.stop();
  }
}
