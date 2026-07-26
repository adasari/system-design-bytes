package org.example.zk.lock;

import java.util.ArrayList;
import java.util.List;


public class Main {

  public static void main(String[] args) {
    int clientCount = 3;
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < clientCount; i++) {
      Thread clientThread = new Thread(new LockClient("Client-" + i));
      threads.add(clientThread);
      clientThread.start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("All clients completed");
  }
}
