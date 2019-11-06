package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStorage {

  void put(String key, InputStream in) throws IOException;

  void put(String key, int chunkNumber, InputStream in) throws IOException;

  InputStream get(String key) throws IOException;

  void delete(String key) throws IOException;

  boolean has(String key);

}
