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
package de.mcs.blobstore.utils;

import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.utils.ByteArrayUtils;

/**
 * @author wklaa_000
 *
 */
public class TransformerHelper {

  public static ChunkEntry transformVLogEntryInfo2ChunkEntry(VLogEntryInfo info, int chunkNumber, String containerName,
      String key) {
    ChunkEntry chunkEntry = new ChunkEntry();
    chunkEntry.setChunkNumber(chunkNumber).setContainerName(containerName)
        .setHash(ByteArrayUtils.bytesAsHexString(info.getHash())).setStart(info.getStart())
        .setStartBinary(info.getStartBinary()).setLength(info.getBinarySize()).setKey(key);
    return chunkEntry;
  }

}
