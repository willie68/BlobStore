/**
 * 
 */
package de.mcs.blobstore.vlog;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.blobstore.Options;
import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.Files;

/**
 * @author w.klaas
 *
 */
class TestVLogFile {

  private static final String BLOBSTORE_PATH = "e:/temp/blobstore/mydb";
  private File filePath;
  private QueuedIDGenerator ids;
  private Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    filePath = new File(BLOBSTORE_PATH);
    if (filePath.exists()) {
      Files.remove(filePath, true);
      Thread.sleep(100);
    }
    filePath.mkdirs();
    options = Options.defaultOptions().setPath(BLOBSTORE_PATH);
  }

  @Test
  void testSingleBin() throws IOException, NoSuchAlgorithmException {
    try (VLogFile vLogFile = new VLogFile(options, 1)) {
      byte[] buffer = new byte[128];
      for (int i = 0; i < buffer.length; i++) {
        buffer[i] = (byte) ('0' + (i % 10));
      }
      // new Random().nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      byte[] byteID = ids.getByteID();
      VLogEntryInfo info;

      Monitor m = MeasureFactory.start("write");
      try {
        info = vLogFile.put(byteID, 1, in);
      } finally {
        m.stop();
      }

      System.out.println(info.toString());

      testFileBin(vLogFile, buffer, in, byteID, info);
    }
    System.out.println(MeasureFactory.asString());
  }

  @Test
  void test1000Bin() throws IOException, InterruptedException, NoSuchAlgorithmException {
    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;
    try (VLogFile vLogFile = new VLogFile(options, 1)) {
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      for (int i = 1; i < 1001; i++) {
        in.reset();
        byte[] id = ids.getByteID();
        String idStr = UUID.nameUUIDFromBytes(id).toString();
        Monitor m = MeasureFactory.start("write");
        try {
          descs.add(id);
          info = vLogFile.put(id, 1, in);
        } finally {
          m.stop();
        }
        if ((i % 100) == 0) {
          System.out.print(".");
        }
        if ((i % 10000) == 0) {
          System.out.println(" " + i);
        }
        infos.put(id, info);
      }
      for (byte[] id : descs) {

        System.out.println(infos.get(id).toString());
        in.reset();

        testFileBin(vLogFile, buffer, in, id, infos.get(id));
      }
    }
    System.out.printf("error on id: %d\r\n", ids.getErrorCount());
    System.out.println(MeasureFactory.asString());
  }

  private void testFileBin(VLogFile vLogFile, byte[] buffer, ByteArrayInputStream in, byte[] byteId, VLogEntryInfo info)
      throws IOException {
    in.reset();
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024 * 2);

    out.reset();
    Monitor m = MeasureFactory.start("readBin");
    try {
      try (InputStream input = vLogFile.get(info.startBinary, info.getBinarySize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray());
    assertTrue(IOUtils.contentEquals(inputStream, in));

    out.reset();
    m = MeasureFactory.start("readPostfix");
    try {
      try (InputStream input = vLogFile.get(info.startPostfix, info.getPostfixSize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    VLogPostFix vLogPostFix = VLogPostFix.fromBytes(out.toByteArray());
    assertNotNull(vLogPostFix);
    assertTrue(Arrays.equals(info.getHash(), vLogPostFix.hash));
    assertEquals(buffer.length, vLogPostFix.length);
  }

}
