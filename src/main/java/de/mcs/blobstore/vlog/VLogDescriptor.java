/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: VLogDescription.java
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

import de.mcs.blobstore.Metadata;

/**
 * @author wklaa_000
 *
 */
public class VLogDescriptor {

  private String key;
  private String family;
  private int chunkno;
  private long retention;
  private long timestamp;
  private Metadata metadata;

  private VLogDescriptor() {

  }

  public static VLogDescriptor create() {
    return new VLogDescriptor();
  }

  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key
   *          the key to set
   */
  public VLogDescriptor setKey(String key) {
    this.key = key;
    return this;
  }

  /**
   * @return the family
   */
  public String getFamily() {
    return family;
  }

  /**
   * @param family
   *          the family to set
   */
  public VLogDescriptor setFamily(String family) {
    this.family = family;
    return this;
  }

  /**
   * @return the chunkno
   */
  public int getChunkno() {
    return chunkno;
  }

  /**
   * @param chunkno
   *          the chunkno to set
   */
  public VLogDescriptor setChunkno(int chunkno) {
    this.chunkno = chunkno;
    return this;
  }

  /**
   * @return the retention
   */
  public long getRetention() {
    return retention;
  }

  /**
   * @param retention
   *          the retention to set
   */
  public VLogDescriptor setRetention(long retention) {
    this.retention = retention;
    return this;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp
   *          the timestamp to set
   */
  public VLogDescriptor setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public VLogDescriptor setMetadata(Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

}
