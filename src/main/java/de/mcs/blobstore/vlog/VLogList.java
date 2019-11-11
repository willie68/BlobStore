package de.mcs.blobstore.vlog;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.mcs.blobstore.BlobsDBException;
import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.Options;

public class VLogList {

  private Logger log = Logger.getLogger(this.getClass());

  private Options options;
  private Map<String, VLog> list;
  private ReentrantLock writeLock = new ReentrantLock();

  public VLogList(Options options) {
    this.options = options;
    this.list = new HashMap<>();
  }

  public VLog getNextAvailableVLog() throws IOException {
    writeLock.lock();
    try {
      VLog vLog = null;
      for (VLog vlog : list.values()) {
        if (vlog.isAvailbleForWriting()) {
          vLog = vlog;
        }
      }
      if (vLog == null) {
        int i = 0;
        File file = null;
        do {
          i++;
          file = VLogFile.getFilePathName(new File(options.getPath()), i);
        } while (file.exists());
        VLogFile vLogFile = new VLogFile(options, i);
        vLog = VLog.wrap(vLogFile);
        vLog.forWriting();
        list.put(vLog.getName(), vLog);
      }
      return vLog;
    } finally {
      writeLock.unlock();
    }
  }

  public VLog getVLog(ChunkEntry chunk) throws BlobsDBException {
    VLog vLog = list.get(chunk.getContainerName());
    if (vLog == null) {
      File file = new File(new File(options.getPath()), chunk.getContainerName());
      if (!file.exists()) {
        throw new BlobsDBException(String.format("vlog not found: %s", file.getName()));
      }
      try {
        VLogFile vLogFile = new VLogFile(file);
        vLog = VLog.wrap(vLogFile);
      } catch (IOException e) {
        throw new BlobsDBException(e);
      }
    }
    vLog.forReading();
    return vLog;
  }

  public void close() {
    for (VLog vLog : list.values()) {
      try {
        vLog.closeFile();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }

}
