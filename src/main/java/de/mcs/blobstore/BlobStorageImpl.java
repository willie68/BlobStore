/**
 * 
 */
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

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
   */
  public BlobStorageImpl(String path) {
    this.path = path;
    initBlobStorage();
  }

  private void initBlobStorage() {
    // TODO Auto-generated method stub

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
