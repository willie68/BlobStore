/**
 * 
 */
package de.mcs.blobstore.utils;

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

}
