/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: VLogPostFix.java
 * EMail: W.Klaas@gmx.de
 * Created: 07.11.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.blobstore.vlog;

import java.nio.ByteBuffer;

import de.mcs.utils.ByteArrayUtils;

/**
 * @author wklaa_000
 *
 */
public class VLogPostFix {
  long length;
  byte[] hash;

  VLogPostFix() {
    length = 0;
    hash = new byte[32];
  }

  String getHash() {
    return ByteArrayUtils.bytesAsHexString(hash);
  }

  ByteBuffer getBytes() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(8 + hash.length);
    buffer.putLong(length);
    buffer.put(hash);
    buffer.flip();
    return buffer;
  }

  public static VLogPostFix fromBytes(byte[] byteArray) {
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    VLogPostFix vLogPostFix = new VLogPostFix();
    vLogPostFix.length = buffer.getLong();
    buffer.get(vLogPostFix.hash);
    return vLogPostFix;
  }
}
