/**
 * 
 */
package de.mcs.blobstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import de.mcs.blobstore.BlobEntry.Status;
import de.mcs.blobstore.utils.IDGenerator;
import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.blobstore.utils.TransformerHelper;
import de.mcs.blobstore.vlog.VLog;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.blobstore.vlog.VLogList;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class BlobStorageImpl implements BlobStorage {

  private static final byte[] KEY_INFIX = "_c".getBytes(StandardCharsets.UTF_8);

  private static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

  private static final int KEY_MAX_LENGTH = 255;

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;

  private List<ColumnFamilyDescriptor> cfDescriptors;
  private List<ColumnFamilyHandle> columnFamilyHandleList;

  private RocksDB db;
  private VLogList vLogList;

  private IDGenerator idGenerator;

  private Lock dbFamilyLock;

  private ScheduledExecutorService executor;

  /**
   * creating a new BLobstorage in the desired path
   * 
   * @param path
   *          where to create/open the blob storage
   * @throws RocksDBException
   */
  public BlobStorageImpl(Options options) throws RocksDBException {
    this.options = options;
    this.dbFamilyLock = new ReentrantLock();
    this.executor = Executors.newScheduledThreadPool(10);

    initBlobStorage();
  }

  private void initBlobStorage() throws RocksDBException {
    idGenerator = new QueuedIDGenerator(1000);
    vLogList = new VLogList(options);
    initRocksDB();
  }

  private void initRocksDB() throws RocksDBException {
    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      cfOpts.optimizeUniversalStyleCompaction();

      cfDescriptors = new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));

      columnFamilyHandleList = new ArrayList<>();
      try (org.rocksdb.Options famOptions = new org.rocksdb.Options()) {
        famOptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        List<byte[]> listColumnFamilies = RocksDB.listColumnFamilies(famOptions, options.getPath());
        for (byte[] bs : listColumnFamilies) {
          cfDescriptors.add(new ColumnFamilyDescriptor(bs, cfOpts));
        }
      }

      try (final DBOptions dboptions = new DBOptions()) {
        dboptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        db = RocksDB.open(dboptions, options.getPath(), cfDescriptors, columnFamilyHandleList);
        db.compactRange();
      } catch (RocksDBException e) {
        throw e;
      }
    }
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
      putDBBlobEntry(family, key, blobEntry);

      // writing binary data
      VLogEntryInfo vLogEntryInfo = vlog.put(key, 1, in);
      ChunkEntry chunkEntry = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfo, 1, vlog.getName(),
          keyString);
      putDBChunkEntry(family, key, chunkEntry);

      String jsonBlobEntry = GsonUtils.getJsonMapper().toJson(blobEntry);
      // writing metadata
      ByteArrayInputStream jsonIn = new ByteArrayInputStream(jsonBlobEntry.getBytes(StandardCharsets.UTF_8));
      VLogEntryInfo vLogEntryInfoJson = vlog.put(key, 0, jsonIn);

      ChunkEntry chunkEntryJson = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfoJson, 0,
          vlog.getName(), keyString);
      putDBChunkEntry(family, key, chunkEntryJson);

    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  @Override
  public InputStream get(String family, byte[] key) throws IOException {
    try {
      BlobEntry entry = getDBBlobEntry(family, key);
      if ((entry == null) || entry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      List<ChunkEntry> chunks = getChunks(family, key);
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

  private List<ChunkEntry> getChunks(String family, byte[] key) throws RocksDBException {
    List<ChunkEntry> list = new ArrayList<>();
    String searchKey = ByteArrayUtils.bytesAsHexString(key);
    ReadOptions readOptions = new ReadOptions();
    try (RocksIterator newIterator = db.newIterator(getColumnFamilyHandle(family), readOptions)) {
      newIterator.seek(key);
      while (newIterator.isValid()) {
        String stringKey = ByteArrayUtils.bytesAsHexString(newIterator.key());
        if (stringKey.startsWith(searchKey)) {
          String json = new String(newIterator.value());
          ChunkEntry chunkEntry = GsonUtils.getJsonMapper().fromJson(json, ChunkEntry.class);
          if (chunkEntry.isRightTyped()) {
            list.add(chunkEntry);
          }
        }
        newIterator.next();
      }
    }
    return list;
  }

  private boolean isVLog(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".vlog");
  }

  private BlobEntry getDBBlobEntry(String family, byte[] key) throws RocksDBException, BlobsDBException {
    String value = dbGet(family, key);
    if (value == null) {
      return null;
    }
    BlobEntry entry = GsonUtils.getJsonMapper().fromJson(value, BlobEntry.class);
    return entry;
  }

  private void putDBBlobEntry(String family, byte[] key, BlobEntry blobEntry) throws RocksDBException {
    String json = GsonUtils.getJsonMapper().toJson(blobEntry);

    dbPut(family, key, json);
  }

  private void putDBChunkEntry(String family, byte[] key, ChunkEntry chunkEntry) throws RocksDBException {
    String json = chunkEntry.toJsonString();

    byte[] newKey = getChunkKey(key, chunkEntry.getChunkNumber());

    dbPut(family, newKey, json);
  }

  private ChunkEntry getDBChunkEntry(String family, byte[] key, int chunkNumber)
      throws RocksDBException, BlobsDBException {

    byte[] newKey = getChunkKey(key, chunkNumber);

    String value = dbGet(family, newKey);
    if (value == null) {
      return null;
    }
    return GsonUtils.getJsonMapper().fromJson(value, ChunkEntry.class);
  }

  private byte[] getChunkKey(byte[] key, int chunkNumber) {
    ByteBuffer newKeyBuffer = ByteBuffer.allocate(1024);
    newKeyBuffer.put(key);
    newKeyBuffer.put(KEY_INFIX);
    newKeyBuffer.putInt(chunkNumber);
    newKeyBuffer.flip();

    byte[] newKey = new byte[newKeyBuffer.limit()];
    newKeyBuffer.get(newKey);
    return newKey;
  }

  @Override
  public void put(String family, byte[] key, int chunkNumber, InputStream in) throws IOException {

  }

  @Override
  public void delete(String family, byte[] key) throws IOException {
    try {
      BlobEntry blobEntry = getDBBlobEntry(family, key);
      if (blobEntry == null || blobEntry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      blobEntry.setStatus(Status.DELETED);
      putDBBlobEntry(family, key, blobEntry);
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }

  }

  @Override
  public boolean has(String family, byte[] key) throws IOException {
    try {
      BlobEntry entry = getDBBlobEntry(family, key);
      return ((entry != null) && !entry.getStatus().equals(Status.DELETED));
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  public boolean isDeleted(String family, byte[] key) throws BlobsDBException {
    try {
      BlobEntry entry = getDBBlobEntry(family, key);
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
    put(DEFAULT_COLUMN_FAMILY, key, in, metadata);
  }

  @Override
  public void put(byte[] key, int chunkNumber, InputStream in) throws IOException {
    put(DEFAULT_COLUMN_FAMILY, key, chunkNumber, in);
  }

  @Override
  public InputStream get(byte[] key) throws IOException {
    return get(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public void delete(byte[] key) throws IOException {
    delete(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public boolean has(byte[] key) throws IOException {
    return has(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(byte[] key) throws IOException {
    return getMetadata(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(String family, byte[] key) throws IOException {
    try {
      BlobEntry blobEntry = getDBBlobEntry(family, key);
      if (blobEntry == null || blobEntry.getStatus().equals(Status.DELETED)) {
        throw new BlobsDBException(String.format("blob not found with key: %s#%s", family, key));
      }
      long length = 0;
      List<ChunkEntry> chunks = getChunks(family, key);
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

  private void dbPut(String family, byte[] key, String value) throws RocksDBException {
    if (family == null) {
      db.put(key, value.getBytes());
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      db.put(columnFamilyHandle, key, value.getBytes());
    }
  }

  public List<String> dbGetAllKeys(String family, byte[] key) throws RocksDBException {
    byte[] bs = null;
    if (family == null) {
    } else {
      List<String> keys = new ArrayList<>();
      try (ReadOptions options = new ReadOptions()) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
        try (RocksIterator newIterator = db.newIterator(columnFamilyHandle, options)) {
          newIterator.seek(new byte[0]);
          while (newIterator.isValid()) {
            keys.add(ByteArrayUtils.bytesAsHexString(newIterator.key()));
            newIterator.next();
          }
        }
      }
      return keys;
    }
    return null;
  }

  private String dbGet(String family, byte[] key) throws RocksDBException {
    byte[] bs = null;
    if (family == null) {
      bs = db.get(key);
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      bs = db.get(columnFamilyHandle, key);
    }
    if (bs != null) {
      return new String(bs);
    }
    return null;
  }

  private void dbDel(String family, byte[] key) throws RocksDBException {
    if (family == null) {
      db.delete(key);
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      db.delete(columnFamilyHandle, key);
    }
  }

  private ColumnFamilyHandle getColumnFamilyHandle(String family) throws RocksDBException {
    ColumnFamilyHandle columnFamilyHandle = getFamilyHandle(family);
    if (columnFamilyHandle == null) {
      dbFamilyLock.lock();
      try {
        columnFamilyHandle = getFamilyHandle(family);
        if (columnFamilyHandle == null) {
          try (ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(family.getBytes(), cfOpts);
            cfDescriptors.add(columnFamilyDescriptor);
            columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
            columnFamilyHandleList.add(columnFamilyHandle);
          }
        }
      } finally {
        dbFamilyLock.unlock();
      }
    }
    return columnFamilyHandle;
  }

  private ColumnFamilyHandle getFamilyHandle(String family) throws RocksDBException {
    ColumnFamilyHandle columnFamilyHandle = null;
    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
      if (Arrays.equals(handle.getName(), family.getBytes())) {
        columnFamilyHandle = handle;
        break;
      }
    }
    return columnFamilyHandle;
  }

  @Override
  public void close() {
    if (db != null) {
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
        columnFamilyHandle.close();
      }
      db.close();
    }
    vLogList.close();
  }

}
