package org.example.zk.lock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class DistributedLock {

  public static final String LOCK_PATH = "/locks";
  public static final String LOCK_PREFIX = "/lock-";

  private ZooKeeper zooKeeper;
  private String currentNode;
  private String watchedNode;
  private String clientName;

  public DistributedLock(String clientName, ZooKeeper zooKeeper) throws Exception {
    this.clientName = clientName;
    this.zooKeeper = zooKeeper;

    Stat stat = zooKeeper.exists(LOCK_PATH, false);
    if (stat == null) {
      zooKeeper.create(LOCK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  public void lock()
      throws Exception {
    // create node
    currentNode = zooKeeper.create(
        LOCK_PATH + LOCK_PREFIX,
        new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL
    );

    System.out.println("[" + clientName + "] Created node: " + currentNode);
    acquireLock();
  }

  private void acquireLock() throws Exception {
    List<String> children = zooKeeper.getChildren(LOCK_PATH, false);
    Collections.sort(children);

    String currentNodeName = currentNode.substring(LOCK_PATH.length() + 1);
    if (currentNodeName.equals(children.get(0))) {
      // current node holding the lock
      return;
    }

    /*
     Watch the predecessor node. why? watching the smallest sequence is also ok, but it may cause herd effect when
     that node is deleted, all nodes will be notified. Watching predecessor node means only one node will be notified
     when the predecessor node is deleted. current node can get lock only one predecessor node is deleted.
     /locks
     ├─ lock-0001
     ├─ lock-0002
     ├─ lock-0003
     ├─ lock-0004

     lock-0004 can acquire lock when all nodes from lock-001 - lock-003 are deleted. so just watch lock-0003
     when lock-0003 is deleted, then
     1. its lock-0004 turn
     2. lock-0003 session lost, watch new node for its turn.
     */
    // Watch the predecessor node
    int index = children.indexOf(currentNodeName);
    watchedNode = children.get(index - 1);
    CountDownLatch latch = new CountDownLatch(1);
    Stat stat = zooKeeper.exists(LOCK_PATH + "/" + watchedNode, event -> {
      if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
        latch.countDown();
      }
    });

    if (stat != null) {
      latch.await();

    }

    acquireLock();
  }

  public void unlock() throws Exception {
    if (currentNode != null) {
      // delete node
      zooKeeper.delete(currentNode, -1);
      currentNode = null;
    }
  }
}
