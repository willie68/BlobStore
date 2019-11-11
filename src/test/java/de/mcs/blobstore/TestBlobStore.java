package de.mcs.blobstore;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.jmeasurement.JMConfig;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.Files;

public class TestBlobStore {

  private static final String MYMETADATVALUE = "mymetadatvalue";
  private static final String MYMETADATA = "mymetadata";
  private static final boolean DELETE_STORE_BEFORE_TEST = true;
  private static final String FAMILY = "MCS_WKLAAS";
  private BlobStorage storage;
  private QueuedIDGenerator ids;
  private File filePath;

  @Before
  public void setUp() throws Exception {
    MeasureFactory.setOption(JMConfig.OPTION_DISABLE_DEVIATION, "true");
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    filePath = new File("e:/temp/blobstore/mydb");
    if (filePath.exists() && DELETE_STORE_BEFORE_TEST) {
      Files.remove(filePath, true);
      Thread.sleep(100);
    }
    filePath.mkdirs();
    storage = new BlobStorageImpl(Options.defaultOptions().setPath(filePath.getAbsolutePath()));
  }

  @After
  public void tearDown() throws Exception {
    storage.close();
  }

  @Test
  public void testSingleFile() throws IOException {
    byte[] buffer = new byte[1024 * 1024];
    new Random().nextBytes(buffer);

    byte[] uuid = ids.getByteID();
    Metadata metadata = new Metadata().setContentLength(0).setContentType("text/simple").setRetention(1234567)
        .setProperty(MYMETADATA, MYMETADATVALUE);
    try (InputStream in = new ByteArrayInputStream(buffer)) {
      storage.put(FAMILY, uuid, in, metadata);
    }
    assertFalse(storage.has(uuid));
    assertTrue(storage.has(FAMILY, uuid));

    Assertions.assertThrows(BlobsDBException.class, () -> {
      Metadata metadataNotStored = storage.getMetadata(uuid);
    });

    Metadata metadataStored = storage.getMetadata(FAMILY, uuid);
    assertNotNull(metadataStored);
    assertEquals(metadata.getContentType(), metadataStored.getContentType());
    assertEquals(buffer.length, metadataStored.getContentLength());
    assertEquals(metadata.getRetention(), metadataStored.getRetention());
    assertEquals(metadata.getProperty(MYMETADATA), metadataStored.getProperty(MYMETADATA));

    Assertions.assertThrows(BlobsDBException.class, () -> {
      InputStream noInputStream = storage.get(uuid);
    });

    try (InputStream inputStream = storage.get(FAMILY, uuid)) {
      try (InputStream inputOrg = new BufferedInputStream(new ByteArrayInputStream(buffer))) {
        assertTrue(IOUtils.contentEquals(inputStream, inputOrg));
      }
    }
  }

  @Test
  public void testCRUD() throws IOException {

    System.out.println("create new blob file");
    byte[] buffer = new byte[128];
    new Random().nextBytes(buffer);

    byte[] uuid = ids.getByteID();
    Metadata metadata = new Metadata().setContentLength(0).setContentType("text/simple").setRetention(1234567)
        .setProperty(MYMETADATA, MYMETADATVALUE);
    try (InputStream in = new ByteArrayInputStream(buffer)) {
      storage.put(FAMILY, uuid, in, metadata);
    }

    System.out.println("read");
    assertFalse(storage.has(uuid));
    assertTrue(storage.has(FAMILY, uuid));

    Assertions.assertThrows(BlobsDBException.class, () -> {
      Metadata metadataNotStored = storage.getMetadata(uuid);
    });

    Metadata metadataStored = storage.getMetadata(FAMILY, uuid);
    assertNotNull(metadataStored);
    assertEquals(metadata.getContentType(), metadataStored.getContentType());
    assertEquals(buffer.length, metadataStored.getContentLength());
    assertEquals(metadata.getRetention(), metadataStored.getRetention());
    assertEquals(metadata.getProperty(MYMETADATA), metadataStored.getProperty(MYMETADATA));

    Assertions.assertThrows(BlobsDBException.class, () -> {
      InputStream noInputStream = storage.get(uuid);
    });

    try (InputStream inputStream = storage.get(FAMILY, uuid)) {
      try (InputStream inputOrg = new BufferedInputStream(new ByteArrayInputStream(buffer))) {
        assertTrue(IOUtils.contentEquals(inputStream, inputOrg));
      }
    }

    System.out.println("delete");
    storage.delete(FAMILY, uuid);
    assertFalse(storage.has(FAMILY, uuid));
    assertTrue(((BlobStorageImpl) storage).isDeleted(FAMILY, uuid));

    Assertions.assertThrows(BlobsDBException.class, () -> {
      storage.getMetadata(FAMILY, uuid);
    });

    Assertions.assertThrows(BlobsDBException.class, () -> {
      storage.get(FAMILY, uuid);
    });

  }

  @Test
  public void test1000() throws Exception {
    byte[] buffer = new byte[1024 * 1024 * 10];

    Map<String, Metadata> myIds = new HashMap<>();

    System.out.println("writing");
    for (int i = 1; i <= 5000; i++) {
      new Random(i).nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);

      byte[] uuid = ids.getByteID();
      Metadata metadata = new Metadata().setContentLength(0).setContentType("text/simple").setRetention(i)
          .setProperty("random", Integer.toString(i));
      myIds.put(ByteArrayUtils.bytesAsHexString(uuid), metadata);
      Monitor m = MeasureFactory.start("write");
      try {
        storage.put(FAMILY, uuid, in, metadata);
      } finally {
        m.stop();
      }

      if ((i % 100) == 0) {
        System.out.print(".");
      }
      if ((i % 10000) == 0) {
        System.out.println(" " + i);
      }
    }

    System.out.println();
    System.out.println("reading");
    int i = 0;
    for (String uuidStr : myIds.keySet()) {
      byte[] uuid = ByteArrayUtils.decodeHex(uuidStr);
      i++;
      if ((i % 100) == 0) {
        System.out.print(".");
      }
      if ((i % 10000) == 0) {
        System.out.println(" " + i);
      }
      Metadata metadata = myIds.get(uuidStr);

      Monitor m = MeasureFactory.start("test");
      try {
        assertTrue(storage.has(FAMILY, uuid));
      } finally {
        m.stop();
      }

      Metadata metadataStored = null;
      m = MeasureFactory.start("read-meta");
      try {
        metadataStored = storage.getMetadata(FAMILY, uuid);
      } finally {
        m.stop();
      }
      assertNotNull(metadataStored);
      assertEquals(metadata.getContentType(), metadataStored.getContentType());
      assertEquals(buffer.length, metadataStored.getContentLength());
      assertEquals(metadata.getRetention(), metadataStored.getRetention());

      new Random(Integer.parseInt(metadataStored.getProperty("random"))).nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      m = MeasureFactory.start("read-bin");
      try (InputStream inputStream = storage.get(FAMILY, uuid)) {
        assertTrue(IOUtils.contentEquals(inputStream, in));
      } finally {
        m.stop();
      }
    }
    System.out.println();
    System.out.printf("error on id: %d\r\n", ids.getErrorCount());
    System.out.println(MeasureFactory.asString());
  }
}
