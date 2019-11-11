/**
 * 
 */
package de.mcs.blobstore;

import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class ChunkEntry {
  private int chunkNumber;
  private String containerName;
  private long start;
  private long startBinary;
  private long length;
  private String hash;

  /**
   * @return the chunkNumber
   */
  public int getChunkNumber() {
    return chunkNumber;
  }

  /**
   * @param chunkNumber
   *          the chunkNumber to set
   */
  public ChunkEntry setChunkNumber(int chunkNumber) {
    this.chunkNumber = chunkNumber;
    return this;
  }

  /**
   * @return the containerName
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * @param containerName
   *          the containerName to set
   */
  public ChunkEntry setContainerName(String containerName) {
    this.containerName = containerName;
    return this;
  }

  /**
   * @return the start
   */
  public long getStart() {
    return start;
  }

  /**
   * @param start
   *          the start to set
   */
  public ChunkEntry setStart(long start) {
    this.start = start;
    return this;
  }

  /**
   * @return the startBinary
   */
  public long getStartBinary() {
    return startBinary;
  }

  /**
   * @param startBinary
   *          the startBinary to set
   */
  public ChunkEntry setStartBinary(long startBinary) {
    this.startBinary = startBinary;
    return this;
  }

  /**
   * @return the hash
   */
  public String getHash() {
    return hash;
  }

  /**
   * @param hash
   *          the hash to set
   * @return
   */
  public ChunkEntry setHash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toJsonString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  /**
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   */
  public void setLength(long length) {
    this.length = length;
  }

}
