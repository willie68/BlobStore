/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: TransformerHelper.java
 * EMail: W.Klaas@gmx.de
 * Created: 09.11.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
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
