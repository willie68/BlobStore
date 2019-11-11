/**
 * 
 */
package de.mcs.blobstore.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author wklaa_000
 *
 */
public class DefaultIDGenerator implements IDGenerator {

  @Override
  public String getID() {
    return UUID.randomUUID().toString();
  }

  @Override
  public ByteBuffer getByteID() {
    UUID id = UUID.randomUUID();
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(id.getMostSignificantBits());
    buf.putLong(id.getLeastSignificantBits());
    return buf;
  }

}
