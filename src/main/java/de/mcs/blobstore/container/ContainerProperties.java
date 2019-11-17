/**
 * Copyright 2019 w.klaas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
