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

import java.io.IOException;

public class ByteArrayUtils {
  public static final int LONGBYTES = 8;

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

  /**
   * Converts a String representing hexadecimal values into an array of bytes of those same values. The
   * returned array will be half the length of the passed String, as it takes two characters to represent any given
   * byte. An exception is thrown if the passed String has an odd number of elements.
   *
   * @param data
   *          A String containing hexadecimal digits
   * @return A byte array containing binary data decoded from the supplied char array.
   * @throws Exception
   * @throws DecoderException
   *           Thrown if an odd number or illegal of characters is supplied
   * @since 1.11
   */
  public static byte[] decodeHex(final String data) throws Exception {
    return decodeHex(data.toCharArray());
  }

  /**
   * Converts an array of characters representing hexadecimal values into an array of bytes of those same values. The
   * returned array will be half the length of the passed array, as it takes two characters to represent any given
   * byte. An exception is thrown if the passed char array has an odd number of elements.
   *
   * @param data
   *          An array of characters containing hexadecimal digits
   * @return A byte array containing binary data decoded from the supplied char array.
   * @throws DecoderException
   *           Thrown if an odd number or illegal of characters is supplied
   */
  public static byte[] decodeHex(final char[] data) throws Exception {

    final int len = data.length;

    if ((len & 0x01) != 0) {
      throw new Exception("Odd number of characters.");
    }

    final byte[] out = new byte[len >> 1];

    // two characters form the hex value.
    for (int i = 0, j = 0; j < len; i++) {
      int f = toDigit(data[j], j) << 4;
      j++;
      f = f | toDigit(data[j], j);
      j++;
      out[i] = (byte) (f & 0xFF);
    }

    return out;
  }

  /**
   * Converts a hexadecimal character to an integer.
   *
   * @param ch
   *          A character to convert to an integer digit
   * @param index
   *          The index of the character in the source
   * @return An integer
   * @throws DecoderException
   *           Thrown if ch is an illegal hex character
   */
  protected static int toDigit(final char ch, final int index) throws Exception {
    final int digit = Character.digit(ch, 16);
    if (digit == -1) {
      throw new Exception("Illegal hexadecimal character " + ch + " at index " + index);
    }
    return digit;
  }

  /**
   * converts a long value into a byte array
   * @param l long value
   * @return byte array
   */
  public static byte[] longToBytes(long l) {
    byte[] result = new byte[LONGBYTES];
    for (int i = LONGBYTES - 1; i >= 0; i--) {
      result[i] = (byte) (l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  /**
   * converts a byte array into a long value
   * @param b byte array
   * @return long
   */
  public static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = 0; i < LONGBYTES; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }
}
