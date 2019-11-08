/**
 * 
 */
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.mcs.blobstore.vlog.VLog;
import de.mcs.blobstore.vlog.VLogDescriptor;
import de.mcs.blobstore.vlog.VLogList;

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
  private VLogList vlogList;

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
    VLogDescriptor vlogDesc = VLogDescriptor.create();
    try (VLog vlog = vlogList.getNextAvailableVLog()) {
      vlog.getvLogFile().put(vlogDesc, in);
    }
  }

  @Override
  public InputStream get(String family, String key) throws IOException {
    return null;
  }

  @Override
  public void put(String family, String key, int chunkNumber, InputStream in) throws IOException {

  }

  @Override
  public void delete(String family, String key) throws IOException {

  }

  @Override
  public boolean has(String family, String key) {
    return false;
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
  public boolean has(String key) {
    return has(DEFAULT_COLUMN_FAMILY, key);
  }
}
