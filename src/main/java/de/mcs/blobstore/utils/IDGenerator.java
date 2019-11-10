/**
 * 
 */
package de.mcs.blobstore.utils;

import java.nio.ByteBuffer;

/**
 * @author wklaa_000
 *
 */
public interface IDGenerator {

  String getID();

  ByteBuffer getByteID();
}
