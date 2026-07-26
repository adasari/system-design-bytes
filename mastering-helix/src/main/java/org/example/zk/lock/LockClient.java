package org.example.zk.lock;

import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class LockClient implements Runnable {

  private String name;

  public LockClient(String name) {
    this.name = name;
  }

  @Override
  public void run() {
    CountDownLatch latch = new CountDownLatch(1);

    try {

      ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, event -> {
        if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
          latch.countDown();
        }
      });

      // wait for connection establishment
      latch.await();

      DistributedLock lock = new DistributedLock(name, zk);
      System.out.println(name + " is trying to acquire the lock.");

      lock.lock();
      System.out.println(name + " acquired lock");

      // sleep
      Thread.sleep(30000);

      System.out.println(name + " released lock");
      zk.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }


  }
}
