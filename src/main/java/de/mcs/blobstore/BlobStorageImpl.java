/**
 * 
 */
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.mcs.blobstore.utils.TransformerHelper;
import de.mcs.blobstore.vlog.VLog;
import de.mcs.blobstore.vlog.VLogDescriptor;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.blobstore.vlog.VLogList;
import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class BlobStorageImpl implements BlobStorage {

  private static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

  private Options options;

  private List<ColumnFamilyDescriptor> cfDescriptors;
  private List<ColumnFamilyHandle> columnFamilyHandleList;

  private RocksDB db;
  private VLogList vLogList;

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
  public void put(String family, String key, InputStream in, Metadata metadata) throws IOException {
    BlobEntry blobEntry = new BlobEntry();
    blobEntry.setFamily(family).setKey(key).setMetadata(metadata).setTimestamp(new Date().getTime())
        .setRetention(metadata.getRetention());

    VLogDescriptor vlogDesc = TransformerHelper.transformBLobEntry2VLogDescriptor(blobEntry, 0);
    try (VLog vlog = vLogList.getNextAvailableVLog()) {
      VLogEntryInfo vLogEntryInfo = vlog.put(vlogDesc, in);
      ChunkEntry chunkEntry = TransformerHelper.transformVLogEntryInfo2ChunkEntry(vLogEntryInfo, 0, vlog.getName());
      blobEntry.addChunkEntry(chunkEntry);

      String json = GsonUtils.getJsonMapper().toJson(blobEntry);

      dbPut(family, key, json);
      System.out.println(blobEntry.toJsonString());
    } catch (RocksDBException e) {
      throw new BlobException(e);
    }
  }

  @Override
  public InputStream get(String family, String key) throws IOException {
    try {
      BlobEntry entry = getDBBlobEntry(family, key);
      List<ChunkEntry> chunks = entry.getChunks();
      if (chunks == null || chunks.size() == 0) {
        throw new BlobException(String.format("chunks not found with key: %s#%s", family, key));
      }
      ChunkEntry chunk = chunks.get(0);
      if (isVLog(chunk)) {
        try (VLog vlog = vLogList.getVLog(chunk)) {
          return vlog.get(chunk.getStartBinary(), chunk.getBinarySize());
        }
      }
      return null;
    } catch (RocksDBException e) {
      throw new BlobException(e);
    }
  }

  private BlobEntry getDBBlobEntry(String family, String key) throws RocksDBException, BlobException {
    String value = dbGet(family, key);
    if (value == null) {
      throw new BlobException(String.format("blob not found with key: %s#%s", family, key));
    }

    BlobEntry entry = GsonUtils.getJsonMapper().fromJson(value, BlobEntry.class);
    return entry;
  }

  private boolean isVLog(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".vlog");
  }

  @Override
  public void put(String family, String key, int chunkNumber, InputStream in) throws IOException {

  }

  @Override
  public void delete(String family, String key) throws IOException {

  }

  @Override
  public boolean has(String family, String key) throws IOException {
    try {
      String value = dbGet(family, key);
      return value != null;
    } catch (RocksDBException e) {
      throw new BlobException(e);
    }
  }

  @Override
  public void put(String key, InputStream in, Metadata metadata) throws IOException {
    put(DEFAULT_COLUMN_FAMILY, key, in, metadata);
  }

  @Override
  public void put(String key, int chunkNumber, InputStream in) throws IOException {
    put(DEFAULT_COLUMN_FAMILY, key, chunkNumber, in);
  }

  @Override
  public InputStream get(String key) throws IOException {
    return get(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public void delete(String key) throws IOException {
    delete(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public boolean has(String key) throws IOException {
    return has(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(String key) throws IOException {
    return getMetadata(DEFAULT_COLUMN_FAMILY, key);
  }

  @Override
  public Metadata getMetadata(String family, String key) throws IOException {
    try {
      BlobEntry blobEntry = getDBBlobEntry(family, key);
      if (blobEntry == null) {
        return null;
      }
      return blobEntry.getMetadata();
    } catch (RocksDBException e) {
      throw new BlobException(e);
    }
  }

  private void dbPut(String family, String key, String value) throws RocksDBException {
    if (family == null) {
      db.put(key.getBytes(), value.getBytes());
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      db.put(columnFamilyHandle, key.getBytes(), value.getBytes());
    }
  }

  private String dbGet(String family, String key) throws RocksDBException {
    byte[] bs = null;
    if (family == null) {
      bs = db.get(key.getBytes());
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      bs = db.get(columnFamilyHandle, key.getBytes());
    }
    if (bs != null) {
      return new String(bs);
    }
    return null;
  }

  private void dbDel(String family, String key) throws RocksDBException {
    if (family == null) {
      db.delete(key.getBytes());
    } else {
      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(family);
      db.delete(columnFamilyHandle, key.getBytes());
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
      System.out.println("create family");
      try (ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(family.getBytes(), cfOpts);
        cfDescriptors.add(columnFamilyDescriptor);
        columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
        columnFamilyHandleList.add(columnFamilyHandle);
      }
    }
    return columnFamilyHandle;
  }
}
