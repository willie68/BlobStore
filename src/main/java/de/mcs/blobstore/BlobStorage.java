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
package de.mcs.blobstore;

import java.io.IOException;
import java.io.InputStream;

public interface BlobStorage {

  void put(byte[] key, InputStream in, Metadata metadata) throws IOException;

  void put(byte[] key, int chunkNumber, byte[] chunk) throws IOException;

  InputStream get(byte[] key) throws IOException;

  void delete(byte[] key) throws IOException;

  boolean has(byte[] key) throws IOException;

  Metadata getMetadata(byte[] key) throws IOException;

  void put(String family, byte[] key, InputStream in, Metadata metadata) throws IOException;

  void put(String family, byte[] key, int chunkNumber, byte[] chunk) throws IOException;

  InputStream get(String family, byte[] key) throws IOException;

  void delete(String family, byte[] key) throws IOException;

  boolean has(String family, byte[] key) throws IOException;

  Metadata getMetadata(String family, byte[] key) throws IOException;

  void close();
}
