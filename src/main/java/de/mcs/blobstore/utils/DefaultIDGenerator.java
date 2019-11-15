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
package de.mcs.blobstore.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author wklaa_000
 *
 */
public class DefaultIDGenerator implements IDGenerator {

  @Override
  public String getID() {
    return UUID.randomUUID().toString();
  }

  @Override
  public byte[] getByteID() {
    UUID id = UUID.randomUUID();
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(id.getMostSignificantBits());
    buf.putLong(id.getLeastSignificantBits());
    return buf.array();
  }

}
