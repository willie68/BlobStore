package de.mcs.blobstore;

import static org.junit.Assert.assertTrue;

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

import de.mcs.utils.Files;

public class TestBlobStore {

  private BlobStorage storage;

  @Before
  public void setUp() throws Exception {
    storage = new BlobStorageImpl("mydb");
  }

  @Test
  public void test() throws IOException {
    File outfile = new File("tmp/outfile.txt");
    outfile.getParentFile().mkdirs();
    byte[] buffer = new byte[4096];
    Random rnd = new Random();
    rnd.nextBytes(buffer);
    try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outfile))) {
      for (int i = 0; i < 128; i++) {
        output.write(buffer);
      }
    }

    String orgHash = Files.computeMD5FromFile(outfile);
    System.out.println("original hash: " + orgHash);

    String uuid = UUID.randomUUID().toString();
    try (InputStream in = new FileInputStream(outfile)) {
      storage.put(uuid, in);
    }
    assertTrue(storage.has(uuid));

    try (InputStream inputStream = storage.get(uuid)) {
      try (InputStream inputOrg = new BufferedInputStream(new FileInputStream(outfile))) {
        assertTrue(IOUtils.contentEquals(inputStream, inputOrg));
      }
    }
  }

}
