package de.mcs.blobstore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.blobstore.utils.GsonUtils;

class TestMetadata {

  private static final String KEY_DOUBLE_VALUE = "DoubleValue";
  private static final Double DOUBLE_VALUE = Double.valueOf(1.234);
  private static final String CONTENT_TYPE = "application/octet-stream";
  private static final long CONTENT_LENGTH = 1234L;

  @BeforeEach
  void setUp() throws Exception {
  }

  @Test
  void testSimpleCreation() {
    Metadata metadata = new Metadata();
    assertNotNull(metadata);
    assertNull(metadata.getContentType());
    assertEquals(0, metadata.getContentLength());
    assertNull(metadata.getProperty("MUCKEFUCK"));
    assertFalse(metadata.hasProperty(Metadata.KEY_CONTENTLENGTH));
    assertFalse(metadata.hasProperty(Metadata.KEY_CONTENTTYPE));
  }

  @Test
  void testFluidCreation() {
    Metadata metadata = new Metadata().setContentLength(1234L).setContentType(CONTENT_TYPE)
        .setProperty(KEY_DOUBLE_VALUE, DOUBLE_VALUE.toString());
    assertNotNull(metadata);
    assertNotNull(metadata.getContentType());
    assertTrue(metadata.hasProperty(Metadata.KEY_CONTENTLENGTH));
    assertTrue(metadata.hasProperty(Metadata.KEY_CONTENTTYPE));
    assertTrue(metadata.hasProperty(KEY_DOUBLE_VALUE));
    assertEquals(CONTENT_LENGTH, metadata.getContentLength());
    assertEquals(CONTENT_TYPE, metadata.getContentType());
    assertNotNull(metadata.getProperty(KEY_DOUBLE_VALUE));
    assertEquals(DOUBLE_VALUE.toString(), metadata.getProperty(KEY_DOUBLE_VALUE));
  }

  @Test
  void testJSONSeriliation() {
    Metadata metadata = new Metadata().setContentLength(1234L).setContentType(CONTENT_TYPE)
        .setProperty(KEY_DOUBLE_VALUE, DOUBLE_VALUE.toString());

    String json = metadata.toString();
    String jsonString = metadata.toJsonString();
    assertEquals(json, jsonString);
    System.out.println(jsonString);

    Metadata newMetadata = GsonUtils.getJsonMapper().fromJson(json, Metadata.class);

    assertEquals(metadata.getContentLength(), newMetadata.getContentLength());
    assertEquals(metadata.getContentType(), newMetadata.getContentType());
    assertEquals(metadata.getProperty(KEY_DOUBLE_VALUE), newMetadata.getProperty(KEY_DOUBLE_VALUE));

  }
}
