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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class RocksDBEngine {
  private static final byte[] KEY_INFIX = "_c".getBytes(StandardCharsets.UTF_8);

  public static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

  private List<ColumnFamilyDescriptor> cfDescriptors;
  private List<ColumnFamilyHandle> columnFamilyHandleList;
  private Lock dbFamilyLock;

  private RocksDB db;
  private Options options;

  public RocksDBEngine(Options options) throws RocksDBException {
    this.options = options;
    init();
    this.dbFamilyLock = new ReentrantLock();
  }

  private void init() throws RocksDBException {
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

  public List<ChunkEntry> getChunks(String family, byte[] key) throws RocksDBException {
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

  public BlobEntry getDBBlobEntry(String family, byte[] key) throws RocksDBException, BlobsDBException {
    String value = dbGet(family, key);
    if (value == null) {
      return null;
    }
    BlobEntry entry = GsonUtils.getJsonMapper().fromJson(value, BlobEntry.class);
    return entry;
  }

  public void putDBBlobEntry(String family, byte[] key, BlobEntry blobEntry) throws RocksDBException {
    String json = GsonUtils.getJsonMapper().toJson(blobEntry);

    dbPut(family, key, json);
  }

  public void putDBChunkEntry(String family, byte[] key, ChunkEntry chunkEntry) throws RocksDBException {
    String json = chunkEntry.toJsonString();

    byte[] newKey = getChunkKey(key, chunkEntry.getChunkNumber());

    dbPut(family, newKey, json);
  }

  public ChunkEntry getDBChunkEntry(String family, byte[] key, int chunkNumber)
      throws RocksDBException, BlobsDBException {

    byte[] newKey = getChunkKey(key, chunkNumber);

    String value = dbGet(family, newKey);
    if (value == null) {
      return null;
    }
    return GsonUtils.getJsonMapper().fromJson(value, ChunkEntry.class);
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

  public void close() {
    if (db != null) {
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
        columnFamilyHandle.close();
      }
      db.close();
    }
  }
}
