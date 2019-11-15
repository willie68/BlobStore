/**
 * 
 */
package de.mcs.blobstore.container;

/**
 * @author w.klaas
 *
 */
public class ContainerProperties {
  private long chunkCount;
  private boolean readOnly;
  private String name;

  public ContainerProperties setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the chunkCount
   */
  public long getChunkCount() {
    return chunkCount;
  }

  /**
   * @param chunkCount
   *          the chunkCount to set
   * @return
   */
  public ContainerProperties setChunkCount(long chunkCount) {
    this.chunkCount = chunkCount;
    return this;
  }

  /**
   * @return the readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * @param readOnly
   *          the readOnly to set
   * @return
   */
  public ContainerProperties setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

}
