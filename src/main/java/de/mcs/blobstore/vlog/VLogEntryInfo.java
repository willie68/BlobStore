/**
 * 
 */
package de.mcs.blobstore.vlog;

import de.mcs.utils.HasherUtils;

/**
 * @author w.klaas
 *
 */
public class VLogEntryInfo {
  long start;
  long startBinary;
  long startPostfix;
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
   * @return the startPostfix
   */
  public long getStartPostfix() {
    return startPostfix;
  }

  /**
   * @return the hash
   */
  public byte[] getHash() {
    return hash;
  }

  public long getBinarySize() {
    return startPostfix - startBinary;
  }

  public long getPostfixSize() {
    return end - startPostfix;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public String toString() {
    return String.format("start: %d, bin: %d, post: %d, end: %d, hash: %s", start, startBinary, startPostfix, end,
        HasherUtils.bytesAsHexString(hash));
  }

}
