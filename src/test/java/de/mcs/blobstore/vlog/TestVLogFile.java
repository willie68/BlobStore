/**
 * 
 */
package de.mcs.blobstore.vlog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.Files;
import de.mcs.utils.GsonUtils;

/**
 * @author w.klaas
 *
 */
class TestVLogFile {

  private File filePath;
  private QueuedIDGenerator ids;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    filePath = new File("e:/temp/blobstore/mydb");
    if (filePath.exists()) {
      Files.remove(filePath, true);
      Thread.sleep(100);
    }
    filePath.mkdirs();
  }

  @Test
  void testSingleBin() throws IOException, NoSuchAlgorithmException {
    try (VLogFile vLogFile = new VLogFile(filePath, 1)) {
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      ByteBuffer byteID = ids.getByteID();
      VLogEntryInfo info = vLogFile.put(byteID.array(), in);

      System.out.println(info.toString());

      testFileBin(vLogFile, buffer, in, byteID, info);
    }
  }

  private void testFileBin(VLogFile vLogFile, byte[] buffer, ByteArrayInputStream in, ByteBuffer byteId,
      VLogEntryInfo info) throws IOException {
    in.reset();
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024 * 2);
    Monitor m = MeasureFactory.start("readDesc");
    try {
      try (InputStream input = vLogFile.get(info.startDescription, info.getDescriptionSize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    out.reset();
    m = MeasureFactory.start("readBin");
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

    inputStream = new ByteArrayInputStream(out.toByteArray());
    VLogPostFix vLogPostFix = GsonUtils.getJsonMapper().fromJson(new InputStreamReader(inputStream), VLogPostFix.class);
    assertNotNull(vLogPostFix);
    assertEquals(info.getHash(), vLogPostFix.hash);
    assertEquals(buffer.length, vLogPostFix.length);
  }

  @Test
  void testSingle() throws IOException, NoSuchAlgorithmException {
    try (VLogFile vLogFile = new VLogFile(filePath, 1)) {
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);

      VLogDescriptor vLogDescSrc = VLogDescriptor.create().setKey(ids.getID()).setFamily("DEFAULT").setChunkno(0)
          .setRetention(0).setTimestamp(new Date().getTime());
      VLogEntryInfo info = vLogFile.put(vLogDescSrc, in);

      System.out.println(info.toString());

      testFile(vLogFile, buffer, in, vLogDescSrc, info);
    }
  }

  private void testFile(VLogFile vLogFile, byte[] buffer, ByteArrayInputStream in, VLogDescriptor vLogDescSrc,
      VLogEntryInfo info) throws IOException {
    in.reset();
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 1024 * 2);
    Monitor m = MeasureFactory.start("readDesc");
    try {
      try (InputStream input = vLogFile.get(info.startDescription, info.getDescriptionSize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }
    ByteArrayInputStream inputStream = new ByteArrayInputStream(out.toByteArray());
    VLogDescriptor vLogDescDest = GsonUtils.getJsonMapper().fromJson(new InputStreamReader(inputStream),
        VLogDescriptor.class);
    assertNotNull(vLogDescDest);
    assertEquals(vLogDescSrc.getChunkno(), vLogDescDest.getChunkno());
    assertEquals(vLogDescSrc.getFamily(), vLogDescDest.getFamily());
    assertEquals(vLogDescSrc.getKey(), vLogDescDest.getKey());
    assertEquals(vLogDescSrc.getRetention(), vLogDescDest.getRetention());
    assertEquals(vLogDescSrc.getTimestamp(), vLogDescDest.getTimestamp());

    out.reset();
    m = MeasureFactory.start("readBin");
    try {
      try (InputStream input = vLogFile.get(info.startBinary, info.getBinarySize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    inputStream = new ByteArrayInputStream(out.toByteArray());
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
    inputStream = new ByteArrayInputStream(out.toByteArray());
    VLogPostFix vLogPostFix = GsonUtils.getJsonMapper().fromJson(new InputStreamReader(inputStream), VLogPostFix.class);
    assertNotNull(vLogPostFix);
    assertEquals(info.getHash(), vLogPostFix.hash);
    assertEquals(buffer.length, vLogPostFix.length);
  }

  @Test
  void test1000() throws IOException, InterruptedException, NoSuchAlgorithmException {
    Map<String, VLogDescriptor> descs = new HashMap<>();

    Map<String, VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;
    try (VLogFile vLogFile = new VLogFile(filePath, 1)) {
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      ByteArrayInputStream in = new ByteArrayInputStream(buffer);
      for (int i = 1; i < 1001; i++) {
        in.reset();
        String id = ids.getID();
        Monitor m = MeasureFactory.start("write");
        try {
          VLogDescriptor desc = VLogDescriptor.create().setKey(id).setFamily("DEFAULT").setChunkno(0).setRetention(0)
              .setTimestamp(new Date().getTime());
          descs.put(id, desc);
          info = vLogFile.put(desc, in);
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
      for (String id : descs.keySet()) {

        System.out.println(infos.get(id).toString());
        in.reset();

        testFile(vLogFile, buffer, in, descs.get(id), infos.get(id));
      }
    }
    System.out.printf("error on id: %d\r\n", ids.getErrorCount());
    System.out.println(MeasureFactory.asString());
  }

}
