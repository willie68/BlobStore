/**
 * 
 */
package de.mcs.blobstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.rocksdb.RocksDBException;

import de.mcs.blobstore.BlobEntry.Status;
import de.mcs.blobstore.utils.TransformerHelper;
import de.mcs.blobstore.vlog.VLog;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.blobstore.vlog.VLogList;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.GsonUtils;
import de.mcs.utils.logging.Logger;

/**
 * @author w.klaas
 *
 */
public class BlobStorageImpl implements BlobStorage {

  private static final int KEY_MAX_LENGTH = 255;

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;

  private VLogList vLogList;

  private ScheduledExecutorService executor;

  private RocksDBEngine rocksDBEngine;

  private VLogCompactor compactor;

  /**
   * creating a new BLobstorage in the desired path
   * 
   * @param path
   *          where to create/open the blob storage
   * @throws RocksDBException
   */
  public BlobStorageImpl(Options options) throws RocksDBException {
    this.options = options;
    this.executor = Executors.newScheduledThreadPool(10);
    this.vLogList = new VLogList(options);
    this.compactor = VLogCompactor.create().withOptions(options).withDB(rocksDBEngine);

    initBlobStorage();

    executor.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        List<VLog> list = vLogList.getList();
        for (Iterator iterator = list.iterator(); iterator.hasNext();) {
          VLog vLog = (VLog) iterator.next();
          if (vLog.getvLogFile().getSize() > options.vlogMaxSize) {
            if (!vLog.hasWriteLock()) {
              vLog.getvLogFile().setReadOnly(true);
              vLogList.remove(vLog);
              log.info("remove vlog %s from writing list.", vLog.getName());
              compactor.addVLog(vLog);
            }
          }
        }
        compactor.startCompaction();
      }
    }, 10, 10, TimeUnit.SECONDS);
  }

  private void initBlobStorage() throws RocksDBException {
    rocksDBEngine = new RocksDBEngine(options);
  }

  @Override
  public void put(String family, byte[] key, InputStream in, Metadata metadata) throws IOException {
    if (key.length > KEY_MAX_LENGTH) {
      throw new BlobsDBException(String.format("key exceeding length of %d bytes", KEY_MAX_LENGTH));
    }
    BlobEntry blobEntry = new BlobEntry();
    String keyString = ByteArrayUtils.bytesAsHexString(key);
    blobEntry.setFamily(family).setKey(keyString).setMetadata(metadata).setTimestamp(new Date().getTime())
        .setRetention(metadata.getRetention()).setStatus(BlobEntry.Status.CREATED);

    try (VLog vlog = vLogList.getNextAvailableVLog()) {
      rocksDBEngine.putDBBlobEntry(family, key, blobEntry);

      // only write the first chunk, if an inputstream is availble
      if (in != null) {
        // writing binary data
        VLogEntryInfo vLogEntryInfo = vlog.put(family, key, 1, in);
        ChunkEntry chunkEntry = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfo, 1, vlog.getName(),
            keyString);
        rocksDBEngine.putDBChunkEntry(family, key, chunkEntry);
      }

      String jsonBlobEntry = GsonUtils.getJsonMapper().toJson(blobEntry);
      // writing metadata
      ByteArrayInputStream jsonIn = new ByteArrayInputStream(jsonBlobEntry.getBytes(StandardCharsets.UTF_8));
      VLogEntryInfo vLogEntryInfoJson = vlog.put(family, key, 0, jsonIn);

      ChunkEntry chunkEntryJson = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfoJson, 0,
          vlog.getName(), keyString);
      rocksDBEngine.putDBChunkEntry(family, key, chunkEntryJson);

    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  @Override
  public InputStream get(String family, byte[] key) throws IOException {
    try {
      BlobEntry entry = rocksDBEngine.getDBBlobEntry(family, key);
      if ((entry == null) || entry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      List<ChunkEntry> chunks = rocksDBEngine.getChunks(family, key);
      if (chunks == null || chunks.size() == 0) {
        throw new BlobsDBException(String.format("chunks not found with key: %s#%s", family, key));
      }
      ChunkEntry chunk = null;
      for (ChunkEntry chunkEntry : chunks) {
        if (chunkEntry.getChunkNumber() == 1) {
          chunk = chunkEntry;
          break;
        }
      }
      if (isVLog(chunk)) {
        VLog vlog = vLogList.getVLog(chunk);
        return vlog.get(chunk.getStartBinary(), chunk.getLength());
      }
      return null;
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  private boolean isVLog(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".vlog");
  }

  @Override
  public void put(String family, byte[] key, int chunkNumber, InputStream in) throws IOException {

  }

  @Override
  public void delete(String family, byte[] key) throws IOException {
    try {
      BlobEntry blobEntry = rocksDBEngine.getDBBlobEntry(family, key);
      if (blobEntry == null || blobEntry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      blobEntry.setStatus(Status.DELETED);
      rocksDBEngine.putDBBlobEntry(family, key, blobEntry);
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }

  }

  @Override
  public boolean has(String family, byte[] key) throws IOException {
    try {
      BlobEntry entry = rocksDBEngine.getDBBlobEntry(family, key);
      return ((entry != null) && !entry.getStatus().equals(Status.DELETED));
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  public boolean isDeleted(String family, byte[] key) throws BlobsDBException {
    try {
      BlobEntry entry = rocksDBEngine.getDBBlobEntry(family, key);
      if (entry == null) {
        return true;
      }
      return entry.getStatus().equals(Status.DELETED);
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  @Override
  public void put(byte[] key, InputStream in, Metadata metadata) throws IOException {
    put(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key, in, metadata);
  }

  @Override
  public void put(byte[] key, int chunkNumber, InputStream in) throws IOException {
    put(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key, chunkNumber, in);
  }

  @Override
  public InputStream get(byte[] key) throws IOException {
    return get(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public void delete(byte[] key) throws IOException {
    delete(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public boolean has(byte[] key) throws IOException {
    return has(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(byte[] key) throws IOException {
    return getMetadata(RocksDBEngine.DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(String family, byte[] key) throws IOException {
    try {
      BlobEntry blobEntry = rocksDBEngine.getDBBlobEntry(family, key);
      if (blobEntry == null || blobEntry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      long length = 0;
      List<ChunkEntry> chunks = rocksDBEngine.getChunks(family, key);
      for (ChunkEntry chunk : chunks) {
        if (chunk.getChunkNumber() > 0) {
          length += chunk.getLength();
        }
      }
      blobEntry.setLength(length);
      blobEntry.getMetadata().setContentLength(length);
      return blobEntry.getMetadata();
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  @Override
  public void close() {
    rocksDBEngine.close();
    vLogList.close();
  }

}
