/**
 * 
 */
package de.mcs.blobstore.utils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

/**
 * @author wklaa_000
 *
 */
public class QueuedIDGenerator implements IDGenerator {

  private Queue<UUID> queue = new ArrayDeque<>();
  private int errorCount = 0;
  private int queuedIdCount;

  public QueuedIDGenerator(int queuedIdCount) {
    this.queuedIdCount = queuedIdCount;
    Thread idThread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (true) {
          while (queue.size() < queuedIdCount) {
            queue.offer(UUID.randomUUID());
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
    UUID id = queue.poll();
    if (id == null) {
      errorCount++;
      id = UUID.randomUUID();
    }
    return id.toString();
  }

  @Override
  public byte[] getByteID() {
    UUID id = queue.poll();
    if (id == null) {
      errorCount++;
      id = UUID.randomUUID();
    }
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(id.getMostSignificantBits());
    buf.putLong(id.getLeastSignificantBits());
    return buf.array();
  }

  public int getErrorCount() {
    return errorCount;
  }
}
