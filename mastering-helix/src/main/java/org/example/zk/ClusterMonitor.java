package org.example.zk;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;


public class ClusterMonitor implements Runnable {

  private static String membershipRoot = "/Members";

  private final String host;
  private final ZooKeeper zk;

  private Watcher connectionWatcher;
  private Watcher childrenWatcher;

  volatile boolean alive = true;

  public ClusterMonitor(String host) throws IOException, InterruptedException, KeeperException {
    this.host = host;
    connectionWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if(event.getType() == Watcher.Event.EventType.None &&
            event.getState() == Watcher.Event.KeeperState.SyncConnected) {
          System.out.printf("\nEvent Received: %s\n", event.toString());
        }
      }
    };

    childrenWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        System.out.printf("\nEvent Received: %s\n", event.toString());
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
          try {
            // Get current list of child nodes
            // reset watch
            System.out.println("Cluster membership change");
            List<String> children = zk.getChildren(membershipRoot, this);
            System.out.println("Children: " + children);
          } catch (KeeperException ex) {
            throw new RuntimeException(ex);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            alive = false;
            throw new RuntimeException(ex);
          }
        }
      }
    };

    zk = new ZooKeeper(host, 2000, connectionWatcher);

    if (zk.exists(membershipRoot, false) == null) {
      zk.create(membershipRoot, "ClusterMonitorRoot".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // Set a watch on the parent znode
    List<String> children = zk.getChildren(membershipRoot, childrenWatcher);
    System.err.println("Members: " + children);
  }

  @Override
  public void run() {
    try {
      synchronized (this) {
        while (alive) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } finally {
      this.close();
    }
  }

  public synchronized void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
//    if (args.length != 1) {
//      System.err.println("Usage: ClusterMonitor <Host:Port>");
//      System.exit(0);
//    }
    String hostPort = "localhost:2181";
    new ClusterMonitor(hostPort).run();
  }
}
