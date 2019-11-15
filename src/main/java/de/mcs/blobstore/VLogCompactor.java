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
package de.mcs.blobstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.mcs.blobstore.vlog.VLog;
import de.mcs.utils.logging.Logger;

public class VLogCompactor {

  public static VLogCompactor create() {
    return new VLogCompactor();
  }

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;
  private RocksDBEngine db;
  private List<VLog> toCompact;

  private VLogCompactor() {
    toCompact = new ArrayList<>();
  }

  public VLogCompactor withOptions(Options options) {
    this.options = options;
    return this;
  }

  public VLogCompactor withDB(RocksDBEngine dbEngine) {
    this.db = dbEngine;
    return this;
  }

  public void addVLog(VLog vLog) {
    toCompact.add(vLog);
  }

  public void startCompaction() throws IOException {
    if (toCompact.size() > 0) {
      // log.info("start compaction of vLog files");
      for (VLog vLog : toCompact) {
        // log.info("vlog: %s", vLog.getName());
        // vLog.getvLogFile().iterator();
      }
    }
  }

}
