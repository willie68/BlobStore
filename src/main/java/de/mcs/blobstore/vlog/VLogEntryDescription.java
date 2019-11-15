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
