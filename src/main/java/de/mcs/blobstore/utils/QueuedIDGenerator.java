/**
 * 
 */
package de.mcs.blobstore.utils;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

/**
 * @author wklaa_000
 *
 */
public class QueuedIDGenerator implements IDGenerator {

  private Queue<String> queue = new ArrayDeque<>();
  private int errorCount = 0;
  private int queuedIdCount;

  public QueuedIDGenerator(int queuedIdCount) {
    this.queuedIdCount = queuedIdCount;
    Thread idThread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (true) {
          while (queue.size() < queuedIdCount) {
            queue.offer(UUID.randomUUID().toString());
          }
          Thread.yield();
        }
      }
    });
    idThread.setDaemon(true);
    idThread.setName(this.getClass().getSimpleName() + "_bg");
    idThread.start();
  }

  @Override
  public String getID() {
    String id = queue.poll();
    if (id == null) {
      errorCount++;
      id = UUID.randomUUID().toString();
    }
    return id;
  }

  public int getErrorCount() {
    return errorCount;
  }
}
