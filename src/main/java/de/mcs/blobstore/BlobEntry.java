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
