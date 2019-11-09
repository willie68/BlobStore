package de.mcs.blobstore.vlog;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.mcs.blobstore.BlobException;
import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.Options;

public class VLogList {

  private Logger log = Logger.getLogger(this.getClass());

  private Options options;
  private Map<String, VLogFile> list;
  private ReentrantLock writeLock = new ReentrantLock();

  public VLogList(Options options) {
    this.options = options;
    this.list = new HashMap<>();
  }

  public VLog getNextAvailableVLog() throws IOException {
    writeLock.lock();
    try {
      VLogFile vLogFile = null;
      for (VLogFile file : list.values()) {
        if (file.isAvailbleForWriting()) {
          vLogFile = file;
        }
      }
      if (vLogFile == null) {
        int i = 0;
        File file = null;
        do {
          file = VLogFile.getFilePathName(new File(options.getPath()), i++);
        } while (file.exists());
        vLogFile = new VLogFile(new File(options.getPath()), i);
        list.put(vLogFile.getName(), vLogFile);
      }
      VLog vlog = VLog.wrap(vLogFile);
      vlog.forWriting();
      return vlog;
    } finally {
      writeLock.unlock();
    }
  }

  public VLog getVLog(ChunkEntry chunk) throws BlobException {
    VLogFile vLogFile = list.get(chunk.getContainerName());
    if (vLogFile == null) {
      File file = new File(new File(options.getPath()), chunk.getContainerName());
      if (!file.exists()) {
        throw new BlobException(String.format("vlog not found: %s", file.getName()));
      }
      try {
        vLogFile = new VLogFile(file);
      } catch (IOException e) {
        throw new BlobException(e);
      }
    }
    VLog vlog = VLog.wrap(vLogFile);
    vlog.forReading();
    return vlog;
  }

  public void close() {
    for (VLogFile vLogFile : list.values()) {
      try {
        vLogFile.close();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }

}
