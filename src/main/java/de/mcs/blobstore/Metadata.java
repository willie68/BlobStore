/**
 * 
 */
package de.mcs.blobstore;

import java.util.HashMap;
import java.util.Map;

import de.mcs.blobstore.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
public class Metadata {

  static final String KEY_CONTENTTYPE = "contentType";
  static final String KEY_CONTENTLENGTH = "contentLength";
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

  @Override
  public String toString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

  public String toJsonString() {
    return GsonUtils.getJsonMapper().toJson(this);
  }

}
