package de.mcs.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class to provide encryption functionalities.
 * 
 * @author s.laurien
 *
 */
public class HasherUtils {

  /**
   * Enumeration to define the encryption algorithm.
   * 
   * @author s.laurien
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
    return bytesAsHexString(hash(type.getMessageDigest(), file));
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
    return bytesAsHexString(hash(type.getMessageDigest(), input));
  }

  /**
   * 
   * @param digest
   * @param path
   * @return
   * @throws IOException
   */
  private static byte[] hash(MessageDigest digest, Path path) throws IOException {
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
  private static byte[] hash(MessageDigest digest, InputStream input) throws IOException {
    byte[] dataBytes = new byte[1024];
    int nread = 0;
    while ((nread = input.read(dataBytes)) != -1)
      digest.update(dataBytes, 0, nread);
    return digest.digest();
  }

  /**
   * 
   * @param bytes
   * @return
   * @throws IOException
   */
  public static String bytesAsHexString(byte[] bytes) {
    return getHex(bytes);
  }

  private static final char[] DIGITS_UPPER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
      'E', 'F' };

  public static String getHex(byte[] data) {
    if (data == null) {
      return null;
    }
    final int l = data.length;
    final char[] out = new char[l << 1];
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = DIGITS_UPPER[(data[i] & 0xF0) >> 4];
      out[j++] = DIGITS_UPPER[(data[i] & 0x0F)];
    }
    return new String(out);
  }

  public static void main(String[] args) {
    try {
      System.out.println(HasherUtils.hash(Algorithm.MD5, Paths.get(URI.create("file:/home/slaurien/Test/mytest.txt"))));
      System.out.println(HasherUtils.hash(Algorithm.MD5, Paths.get(URI.create("file:/home/slaurien/Test/mytest.txt"))));
      System.out
          .println(HasherUtils.hash(Algorithm.MD5, Paths.get(URI.create("file:/home/slaurien/Test/mytest2.txt"))));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
