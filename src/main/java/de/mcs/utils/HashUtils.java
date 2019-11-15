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
package de.mcs.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class to provide encryption functionalities.
 * 
 * @author w.klaas
 *
 */
public class HashUtils {

  /**
   * Enumeration to define the encryption algorithm.
   * 
   * @author w.klaas
   *
   */
  public static enum Algorithm {
    MD5 {
      @Override
      public MessageDigest getMessageDigest() {
        return getDigestByName("MD5");
      }
    },
    SHA_256 {
      @Override
      public MessageDigest getMessageDigest() {
        return getDigestByName("SHA-256");
      }
    },
    SHA_512 {
      @Override
      public MessageDigest getMessageDigest() {
        return getDigestByName("SHA-512");
      }
    };

    /**
     * Gets the MessageDigest instance from the given name.
     * 
     * @param name
     *          The native name of the algorithm.
     * @return The MessageDigest instance.
     */
    private static MessageDigest getDigestByName(String name) {
      try {
        return MessageDigest.getInstance(name);
      } catch (NoSuchAlgorithmException e) {
        // Should never happen!
      }
      return null;
    }

    /**
     * Abstract method to supply the different native MessageDigest instance from each enumeration.
     * 
     * @return The MessageDigest instance.
     */
    public abstract MessageDigest getMessageDigest();
  }

  /**
   * Hashes the given file with the specified algorithm type.
   * 
   * @param type
   *          The algorithm type.
   * @param file
   *          The file to hash.
   * @return The hex string from the hash result.
   * @throws IOException
   *           On failure!
   */
  public static String hash(Algorithm type, Path file) throws IOException {
    return ByteArrayUtils.bytesAsHexString(hash(type.getMessageDigest(), file));
  }

  /**
   * Hashes the given BufferedInputStream with the specified algorithm type.
   * 
   * @param type
   *          The algorithm type.
   * @param input
   *          The input stream to read.
   * @return The hex string from the hash result.
   * @throws IOException
   *           On failure!
   */
  public static String hash(Algorithm type, InputStream input) throws IOException {
    return ByteArrayUtils.bytesAsHexString(hash(type.getMessageDigest(), input));
  }

  /**
   * 
   * @param digest
   * @param path
   * @return
   * @throws IOException
   */
  public static byte[] hash(MessageDigest digest, Path path) throws IOException {
    digest.reset();
    try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(path.toFile()))) {
      return hash(digest, fis);
    }
  }

  /**
   * 
   * @param digest
   * @param input
   * @return
   * @throws IOException
   */
  public static byte[] hash(MessageDigest digest, InputStream input) throws IOException {
    digest.reset();
    byte[] dataBytes = new byte[1024];
    int nread = 0;
    while ((nread = input.read(dataBytes)) != -1)
      digest.update(dataBytes, 0, nread);
    return digest.digest();
  }

}
