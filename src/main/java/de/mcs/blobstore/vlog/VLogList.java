package de.mcs.blobstore.vlog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.mcs.blobstore.BlobsDBException;
import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.Options;
import de.mcs.utils.caches.KeyAlreadyExistsException;
import de.mcs.utils.caches.ObjectCache;
import de.mcs.utils.caches.ObjectCache.ObjectListener;

public class VLogList {

  private Logger log = Logger.getLogger(this.getClass());

  private Options options;
  private Map<String, VLog> vLogMap;
  private ReentrantLock writeLock = new ReentrantLock();
  private ObjectCache<VLog> readMap;

  public VLogList(Options options) {
    this.options = options;
    this.vLogMap = new HashMap<>();
    this.readMap = new ObjectCache<>(100);
    init();
  }

  private void init() {
    readMap.registerObjectListener(new ObjectListener<VLog>() {

      @Override
      public void onDelete(VLog item) {
        try {
          log.debug(String.format("remove %s from read cache", item.getName()));
          item.closeFile();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });
    readMap.startCleanupTask(10000, "vLogReadMap");
  }

  public VLog getNextAvailableVLog() throws IOException {
    writeLock.lock();
    try {
      VLog vLog = getAvailableVLogForWriting();
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
        synchronized (vLogMap) {
          vLogMap.put(vLog.getName(), vLog);
        }
      }
      return vLog;
    } finally {
      writeLock.unlock();
    }
  }

  private VLog getAvailableVLogForWriting() {
    synchronized (vLogMap) {
      for (VLog vLog : vLogMap.values()) {
        if (vLog.isAvailbleForWriting()) {
          return vLog;
        }
      }
    }
    return null;
  }

  public VLog getVLog(ChunkEntry chunk) throws BlobsDBException {
    String containerName = chunk.getContainerName();
    VLog vLog = vLogMap.get(containerName);
    if (vLog == null) {
      vLog = readMap.getObjectFromExternalKey(containerName);
      if (vLog == null) {
        File file = new File(new File(options.getPath()), containerName);
        if (!file.exists()) {
          throw new BlobsDBException(String.format("vlog not found: %s", file.getName()));
        }
        try {
          VLogFile vLogFile = new VLogFile(options, file).setReadOnly(true);
          vLog = VLog.wrap(vLogFile);
        } catch (IOException e) {
          throw new BlobsDBException(e);
        }
      }
      try {
        readMap.addObject(containerName, vLog);
      } catch (KeyAlreadyExistsException e) {
        // should never appear
      }
    }
    vLog.forReading();
    return vLog;
  }

  public List<VLog> getList() {
    List<VLog> list = new ArrayList<>();
    synchronized (vLogMap) {
      for (VLog vLog : vLogMap.values()) {
        list.add(vLog);
      }
    }
    return list;
  }

  public void remove(VLog vLog) {
    synchronized (vLogMap) {
      if (vLogMap.containsValue(vLog)) {
        vLogMap.remove(vLog.getName());
      }
    }
  }

  public void close() {
    for (VLog vLog : vLogMap.values()) {
      try {
        vLog.closeFile();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }

}
