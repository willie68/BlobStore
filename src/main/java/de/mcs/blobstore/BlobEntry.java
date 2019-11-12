/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: BlobEntry.java
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
package de.mcs.blobstore;

import de.mcs.utils.GsonUtils;

/**
 * @author wklaa_000
 *
 */
public class BlobEntry {
  enum Status {
    CREATED, DELETED
  }

  private String key;
  private String family;
  private long retention;
  private long timestamp;
  private Metadata metadata;
  private long length;
  private int status;
  private String _type = this.getClass().getSimpleName();

  public BlobEntry() {
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
  public BlobEntry setKey(String key) {
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
  public BlobEntry setFamily(String family) {
    this.family = family;
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
  public BlobEntry setRetention(long retention) {
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
  public BlobEntry setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * @return the metadata
   */
  public Metadata getMetadata() {
    return metadata;
  }

  /**
   * @param metadata
   *          the metadata to set
   */
  public BlobEntry setMetadata(Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   */
  public BlobEntry setLength(long length) {
    this.length = length;
    return this;
  }

  /**
   * @return the state
   */
  public Status getStatus() {
    return Status.values()[status];
  }

  /**
   * @param state
   *          the state to set
   * @return
   */
  public BlobEntry setStatus(Status status) {
    this.status = status.ordinal();
    return this;
  }

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toJsonString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  /**
   * @return the _type
   */
  public String get_type() {
    return _type;
  }

  public boolean isRightTyped() {
    return this.getClass().getSimpleName().equals(_type);
  }

}
