package de.mcs.blobstore.vlog;

import java.io.File;
import java.io.IOException;

import de.mcs.blobstore.Options;

public class VLogList {

  private Options options;

  public VLogList(Options options) {
    this.options = options;
  }

  public VLog getNextAvailableVLog() throws IOException {
    VLogFile vLogFile = new VLogFile(new File(options.getPath()), 1);
    VLog vlog = VLog.wrap(vLogFile);
    vlog.forWriting();
    return vlog;
  }

}
