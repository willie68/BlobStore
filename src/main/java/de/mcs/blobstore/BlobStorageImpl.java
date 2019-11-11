/**
 * 
 */
package de.mcs.blobstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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

  private static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

  private Logger log = Logger.getLogger(this.getClass());
  private Options options;

  private List<ColumnFamilyDescriptor> cfDescriptors;
  private List<ColumnFamilyHandle> columnFamilyHandleList;

  private RocksDB db;
  private VLogList vLogList;

  private IDGenerator idGenerator;

  /**
   * creating a new BLobstorage in the desired path
   * 
   * @param path
   *          where to create/open the blob storage
   * @throws RocksDBException
   */
  public BlobStorageImpl(Options options) throws RocksDBException {
    this.options = options;
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
    if (key.length > 32) {
      throw new BlobsDBException("key exceeding length of 32 bytes");
    }
    BlobEntry blobEntry = new BlobEntry();
    blobEntry.setFamily(family).setKey(ByteArrayUtils.bytesAsHexString(key)).setMetadata(metadata)
        .setTimestamp(new Date().getTime()).setRetention(metadata.getRetention()).setStatus(BlobEntry.Status.CREATED);

    try (VLog vlog = vLogList.getNextAvailableVLog()) {
      // writing binary data
      VLogEntryInfo vLogEntryInfo = vlog.put(key, 1, in);
      ChunkEntry chunkEntry = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfo, 1, vlog.getName());
      blobEntry.addChunkEntry(chunkEntry);

      String jsonBlobEntry = GsonUtils.getJsonMapper().toJson(blobEntry);
      // writing metadata
      ByteArrayInputStream jsonIn = new ByteArrayInputStream(jsonBlobEntry.getBytes(StandardCharsets.UTF_8));
      VLogEntryInfo vLogEntryInfoJson = vlog.put(key, 0, jsonIn);

      ChunkEntry chunkEntryJson = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfoJson, 0,
          vlog.getName());
      blobEntry.addChunkEntry(chunkEntryJson);

      putDBBlobEntry(family, key, blobEntry);
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
      List<ChunkEntry> chunks = entry.getChunks();
      if (chunks == null || chunks.size() == 0) {
        throw new BlobsDBException(String.format("chunks not found with key: %s#%s", family, key));
      }
      ChunkEntry chunk = chunks.get(0);
      if (isVLog(chunk)) {
        VLog vlog = vLogList.getVLog(chunk);
        return vlog.get(chunk.getStartBinary(), chunk.getLength());
      }
      return null;
    } catch (RocksDBException e) {
      throw new BlobsDBException(e);
    }
  }

  private BlobEntry getDBBlobEntry(String family, byte[] key) throws RocksDBException, BlobsDBException {
    String value = dbGet(family, key);
    if (value == null) {
      return null;
    }
    BlobEntry entry = GsonUtils.getJsonMapper().fromJson(value, BlobEntry.class);
    return entry;
  }

  private boolean isVLog(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".vlog");
  }

  private void putDBBlobEntry(String family, byte[] key, BlobEntry blobEntry) throws RocksDBException {
    String json = GsonUtils.getJsonMapper().toJson(blobEntry);

    dbPut(family, key, json);
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
    ColumnFamilyHandle columnFamilyHandle = null;
    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
      if (Arrays.equals(handle.getName(), family.getBytes())) {
        columnFamilyHandle = handle;
        break;
      }
    }
    if (columnFamilyHandle == null) {
      try (ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(family.getBytes(), cfOpts);
        cfDescriptors.add(columnFamilyDescriptor);
        columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
        columnFamilyHandleList.add(columnFamilyHandle);
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
