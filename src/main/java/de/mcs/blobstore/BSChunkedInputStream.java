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
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;

import de.mcs.blobstore.vlog.VLog;
import de.mcs.blobstore.vlog.VLogFile;
import de.mcs.blobstore.vlog.VLogList;

/**
 * @author w.klaas
 *
 */
public class BSChunkedInputStream extends InputStream {

  private List<ChunkEntry> chunks;
  private int index;
  private long positionInChunk;
  private int chunkIndex;
  private long position;
  private InputStream in;
  private VLogList vLogList;

  /**
   * @param chunks2
   * @param vLogList
   * 
   */
  public BSChunkedInputStream(VLogList vLogList, List<ChunkEntry> chunks) {
    this.vLogList = vLogList;
    this.chunks = chunks;
    chunks.sort(new Comparator<ChunkEntry>() {

      @Override
      public int compare(ChunkEntry o1, ChunkEntry o2) {
        return Integer.compare(o1.getChunkNumber(), o2.getChunkNumber());
      }
    });
    position = 0;
    chunkIndex = 0;
    positionInChunk = 0;
    in = null;
  }

  @Override
  public int read() throws IOException {
    if (in == null || in.available() == 0) {
      if (!openNextChunk()) {
        return -1;
      }
    }
    return in.read();
  }

  private boolean openNextChunk() throws IOException {
    chunkIndex++;
    if (chunkIndex >= chunks.size()) {
      return false;
    }
    ChunkEntry chunk = chunks.get(chunkIndex);
    if (VLogFile.isVLog(chunk)) {
      if (in != null) {
        in.close();
      }
      VLog vlog = vLogList.getVLog(chunk);
      in = vlog.get(chunk.getStartBinary(), chunk.getLength());
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
