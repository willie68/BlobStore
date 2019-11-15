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
