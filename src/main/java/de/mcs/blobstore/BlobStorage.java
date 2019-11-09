package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStorage {

  void put(String key, InputStream in, Metadata metadata) throws IOException;

  void put(String key, int chunkNumber, InputStream in) throws IOException;

  InputStream get(String key) throws IOException;

  void delete(String key) throws IOException;

  boolean has(String key) throws IOException;

  Metadata getMetadata(String key) throws IOException;

  void put(String family, String key, InputStream in, Metadata metadata) throws IOException;

  void put(String family, String key, int chunkNumber, InputStream in) throws IOException;

  InputStream get(String family, String key) throws IOException;

  void delete(String family, String key) throws IOException;

  boolean has(String family, String key) throws IOException;

  Metadata getMetadata(String family, String key) throws IOException;

  void close();
}
