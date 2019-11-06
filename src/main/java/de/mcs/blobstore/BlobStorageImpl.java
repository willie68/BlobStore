/**
 * 
 */
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author w.klaas
 *
 */
public class BlobStorageImpl implements BlobStorage {

  private String path;

  /**
   * creating a new BLobstorage in the desired path
   * 
   * @param path
   *          where to create/open the blob storage
   * @throws RocksDBException
   */
  public BlobStorageImpl(String path) throws RocksDBException {
    this.path = path;
    initBlobStorage();
  }

  private void initBlobStorage() throws RocksDBException {
    RocksDB db = RocksDB.open(path);
  }

  @Override
  public void put(String key, InputStream in) throws IOException {

  }

  @Override
  public InputStream get(String key) throws IOException {
    return null;
  }

  @Override
  public void put(String key, int chunkNumber, InputStream in) throws IOException {

  }

  @Override
  public void delete(String key) throws IOException {

  }

  @Override
  public boolean has(String key) {
    return false;
  }
}
