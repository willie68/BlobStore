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
/**
 * 
 */
package de.mcs.blobstore;

import java.util.HashMap;
import java.util.Map;

import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class Metadata {

  static final String KEY_CONTENTTYPE = "contentType";
  static final String KEY_CONTENTLENGTH = "contentLength";
  static final String KEY_RETENTION = "retention";
  private Map<String, String> properties;

  public Metadata() {
    properties = new HashMap<>();
  }

  public boolean hasProperty(String key) {
    return properties.containsKey(key);
  }

  public Metadata setProperty(String key, String value) {
    properties.put(key, value);
    return this;
  }

  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return the contentType
   */
  public String getContentType() {
    if (hasProperty(KEY_CONTENTTYPE)) {
      return getProperty(KEY_CONTENTTYPE).toString();
    }
    return null;
  }

  /**
   * @param contentType
   *          the contentType to set
   * @return
   */
  public Metadata setContentType(String contentType) {
    setProperty(KEY_CONTENTTYPE, contentType);
    return this;
  }

  /**
   * @return the contentLength
   */
  public long getContentLength() {
    if (hasProperty(KEY_CONTENTLENGTH)) {
      return Long.parseLong(getProperty(KEY_CONTENTLENGTH));
    }
    return 0;
  }

  /**
   * @param contentLength
   *          the contentLength to set
   * @return
   */
  public Metadata setContentLength(long contentLength) {
    setProperty(KEY_CONTENTLENGTH, Long.toString(contentLength));
    return this;
  }

  /**
   * 
   * @return the settet retention of thsi file, 0 no retention is set.
   */
  public long getRetention() {
    if (hasProperty(KEY_RETENTION)) {
      return Long.parseLong(getProperty(KEY_RETENTION));
    }
    return 0;
  }

  /**
   * 
   * @param retention setting the retention of this file
   * @return metadata object
   */
  public Metadata setRetention(long retention) {
    setProperty(KEY_RETENTION, Long.toString(retention));
    return this;
  }

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toJsonString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

}
