/**
 * 
 */
package de.mcs.blobstore.vlog;

/**
 * @author w.klaas
 *
 */
public class VLogEntryInfo {
  long start;
  long startDescription;
  long startBinary;
  long startPostfix;
  long end;
  String hash;

  /**
   * @return the start
   */
  public long getStart() {
    return start;
  }

  /**
   * @return the startDescription
   */
  public long getStartDescription() {
    return startDescription;
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
  public String getHash() {
    return hash;
  }

  public long getDescriptionSize() {
    return startBinary - startDescription;
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
    return String.format("start: %d, desc: %d, bin: %d, post: %d, end: %d, hash: %s", start, startDescription,
        startBinary, startPostfix, end, hash);
  }

}
