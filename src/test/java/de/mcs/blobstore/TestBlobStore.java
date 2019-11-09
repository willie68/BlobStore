package de.mcs.blobstore;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.utils.Files;

public class TestBlobStore {

  private static final boolean DELETE_STORE_BEFORE_TEST = true;
  private static final String FAMILY = "MCS_WKLAAS";
  private BlobStorage storage;
  private QueuedIDGenerator ids;
  private File filePath;

  @Before
  public void setUp() throws Exception {
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

  @Test
  public void test() throws IOException {
    File outfile = new File("tmp/outfile.txt");
    outfile.getParentFile().mkdirs();
    byte[] buffer = new byte[128];
    Random rnd = new Random();
    rnd.nextBytes(buffer);
    try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outfile))) {
      output.write(buffer);
    }

    String orgHash = Files.computeMD5FromFile(outfile);
    System.out.println("original hash: " + orgHash);

    String uuid = UUID.randomUUID().toString();
    Metadata metadata = new Metadata().setContentLength(0).setContentType("text/simple").setRetention(1234567);
    try (InputStream in = new FileInputStream(outfile)) {
      storage.put(FAMILY, uuid, in, metadata);
    }
    assertFalse(storage.has(uuid));
    assertTrue(storage.has(FAMILY, uuid));

    Assertions.assertThrows(BlobException.class, () -> {
      Metadata metadataNotStored = storage.getMetadata(uuid);
    });

    Metadata metadataStored = storage.getMetadata(FAMILY, uuid);
    assertNotNull(metadataStored);
    assertEquals(metadata.getContentType(), metadataStored.getContentType());
    assertEquals(buffer.length, metadataStored.getContentLength());
    assertEquals(metadata.getRetention(), metadataStored.getRetention());

    Assertions.assertThrows(BlobException.class, () -> {
      InputStream noInputStream = storage.get(uuid);
    });

    try (InputStream inputStream = storage.get(FAMILY, uuid)) {
      try (InputStream inputOrg = new BufferedInputStream(new FileInputStream(outfile))) {
        assertTrue(IOUtils.contentEquals(inputStream, inputOrg));
      }
    }
  }

}
