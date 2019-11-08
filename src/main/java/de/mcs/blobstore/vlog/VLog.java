/**
 * 
 */
package de.mcs.blobstore.vlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author w.klaas
 *
 */
public class VLog implements Closeable {

  public static VLog wrap(VLogFile vLogFile) {
    return new VLog().setVLogFile(vLogFile);
  }

  private VLogFile vLogFile;
  private Lock writeLock = null;

  private VLog() {
  }

  private VLog setVLogFile(VLogFile vLogFile) {
    this.vLogFile = vLogFile;
    return this;
  }

  public void forWriting() {
    writeLock = new ReentrantLock();
    writeLock.lock();
  }

  @Override
  public void close() throws IOException {
    if (writeLock != null) {
      writeLock.unlock();
    }
  }

  /**
   * @return the vLogFile
   */
  public VLogFile getvLogFile() {
    return vLogFile;
  }

}
