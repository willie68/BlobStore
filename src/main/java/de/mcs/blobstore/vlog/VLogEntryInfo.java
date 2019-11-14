/**
 * 
 */
package de.mcs.blobstore.vlog;

import de.mcs.utils.ByteArrayUtils;

/**
 * @author w.klaas
 *
 */
public class VLogEntryInfo {
  long start;
  long startBinary;
  long end;
  byte[] hash;

  /**
   * @return the start
   */
  public long getStart() {
    return start;
  }

  /**
   * @return the startBinary
   */
  public long getStartBinary() {
    return startBinary;
  }

  /**
   * @return the hash
   */
  public byte[] getHash() {
    return hash;
  }

  public long getBinarySize() {
    return end - startBinary + 1;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public String toString() {
    return String.format("start: %d, bin: %d, end: %d, hash: %s", start, startBinary, end,
        ByteArrayUtils.bytesAsHexString(hash));
  }

  public long getDescriptionSize() {
    return startBinary - start;
  }

}
