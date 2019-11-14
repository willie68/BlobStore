/**
 * 
 */
package de.mcs.blobstore.vlog;

import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class VLogEntryDescription {
  String containerName;
  String family;
  byte[] key;
  int chunkNumber;
  long start;
  long startBinary;
  long startDescription;
  long end;
  byte[] hash;
  long length;

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
   * @return the startDescription
   */
  public long getStartDescription() {
    return startDescription;
  }

  /**
   * @return the hash
   */
  public byte[] getHash() {
    return hash;
  }

  public long getBinarySize() {
    return length;
  }

  public long getDescriptionSize() {
    return end - startDescription;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toJsonString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  /**
   * @return the containerName
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * @return the family
   */
  public String getFamily() {
    return family;
  }

  /**
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * @return the chunkNumber
   */
  public int getChunkNumber() {
    return chunkNumber;
  }

  /**
   * @return the length
   */
  public long getLength() {
    return length;
  }

}
