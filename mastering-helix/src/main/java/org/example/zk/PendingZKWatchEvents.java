package org.example.zk;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class PendingZKWatchEvents implements Watcher {
  private static final String zkHost = "localhost:2181";
  private static AtomicInteger _eventCount = new AtomicInteger(0);

  public ZooKeeper zk;
  private ThreadPoolExecutor executor;

  public PendingZKWatchEvents() throws Exception {
    this.zk = new ZooKeeper(zkHost, 20000, this);
    executor =  new ThreadPoolExecutor(
            1, 1,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(3),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
  }

  public static void main(String[] args) throws Exception {
    PendingZKWatchEvents watcher = new PendingZKWatchEvents();
    watcher.zk.exists("/newnode", watcher);
    while (true) {
      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    byte[] data = null;
    try {
      // Re-register the watch immediately
      data = zk.getData("/newnode", true, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Queue size: " + executor.getQueue().size());
    final String eventData = new String(data);
    executor.submit(() -> {
      System.out.println("Processing event data: " + eventData + " | Event count: " + _eventCount.incrementAndGet());
      try {
        Thread.sleep(25000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

    });
  }
}
