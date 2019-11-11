package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStorage {

  void put(byte[] key, InputStream in, Metadata metadata) throws IOException;

  void put(byte[] key, int chunkNumber, InputStream in) throws IOException;

  InputStream get(byte[] key) throws IOException;

  void delete(byte[] key) throws IOException;

  boolean has(byte[] key) throws IOException;

  Metadata getMetadata(byte[] key) throws IOException;

  void put(String family, byte[] key, InputStream in, Metadata metadata) throws IOException;

  void put(String family, byte[] key, int chunkNumber, InputStream in) throws IOException;

  InputStream get(String family, byte[] key) throws IOException;

  void delete(String family, byte[] key) throws IOException;

  boolean has(String family, byte[] key) throws IOException;

  Metadata getMetadata(String family, byte[] key) throws IOException;

  void close();
}
