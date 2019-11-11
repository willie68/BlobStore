/**
 * 
 */
package de.mcs.blobstore.vlog;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
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
  private Lock readLock = null;

  private VLog() {
    writeLock = new ReentrantLock();
  }

  private VLog setVLogFile(VLogFile vLogFile) {
    this.vLogFile = vLogFile;
    return this;
  }

  public boolean forWriting() {
    return writeLock.tryLock();
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

  public String getName() {
    return vLogFile.getName();
  }

  public VLogEntryInfo put(byte[] key, int chunk, InputStream in) throws IOException {
    return getvLogFile().put(key, chunk, in);
  }

  public void forReading() {
  }

  public InputStream get(long startBinary, long binarySize) throws IOException {
    return vLogFile.get(startBinary, binarySize);
  }

  public boolean isAvailbleForWriting() {
    return vLogFile.isAvailbleForWriting() && forWriting();
  }

  public void closeFile() throws IOException {
    vLogFile.close();
  }

}
