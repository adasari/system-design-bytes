package org.example.zk;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;


public class CuratorExample {

  private static final String zkHost = "localhost:2181";
  private static AtomicInteger _eventCount = new AtomicInteger(0);

  public static void main(String[] args) throws Exception {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            1, 1,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

    CuratorFramework client = CuratorFrameworkFactory.newClient(
        zkHost,
        new ExponentialBackoffRetry(1000, 3)
    );
    client.start();

    CuratorCache cache = CuratorCache.build(client, "/myNode");

    cache.listenable().addListener(
        CuratorCacheListener.builder()
            .forChanges((oldNode, node) -> {
              System.out.println("Queue size: " + executor.getQueue().size());

              executor.submit(() -> {
                System.out.println(
                    "Processing event: " + node.getPath() +
                        " | Event count: " + _eventCount.incrementAndGet()
                );
                try {
                  Thread.sleep(500000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
            })
            .forDeletes(oldNode -> {
              System.out.println("Node deleted");
            })
            .build()
    );

    cache.start();

    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
