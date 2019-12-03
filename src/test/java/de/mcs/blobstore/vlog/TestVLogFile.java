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
package de.mcs.blobstore.vlog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import de.mcs.blobstore.Options;
import de.mcs.blobstore.utils.QueuedIDGenerator;
import de.mcs.jmeasurement.JMConfig;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.Files;

/**
 * @author w.klaas
 *
 */
@TestMethodOrder(OrderAnnotation.class)
class TestVLogFile {

  private static final int MAX_DOCS = 1000;
  private static final String FAMILY = "EASY";
  private static final String BLOBSTORE_PATH = "e:/temp/blobstore/mydb";
  private static final boolean DELETE_BEFORE_TEST = true;
  private File filePath;
  private QueuedIDGenerator ids;
  private Options options;

  /**
   * @throws java.lang.Exception
   */
  @BeforeEach
  void setUp() throws Exception {
    MeasureFactory.setOption(JMConfig.OPTION_DISABLE_DEVIATION, "true");
    ids = new QueuedIDGenerator(1000);
    Thread.sleep(1000);

    if (DELETE_BEFORE_TEST) {
      deleteFolder();
    }
    options = Options.defaultOptions().setPath(BLOBSTORE_PATH).setVlogMaxChunkCount(10000)
        .setVlogMaxSize(2048L * 1024L * 1024L);
  }

  private void deleteFolder() throws IOException, InterruptedException {
    filePath = new File(BLOBSTORE_PATH);
    if (filePath.exists()) {
      Files.remove(filePath, true);
      Thread.sleep(100);
    }
    filePath.mkdirs();
  }

  @Order(1)
  @Test
  void testSingleBin() throws IOException, NoSuchAlgorithmException {
    System.out.println("test single bin");
    try (VLogFile vLogFile = new VLogFile(options, 1)) {
      byte[] buffer = new byte[128];
      for (int i = 0; i < buffer.length; i++) {
        if ((i % 10) == 0) {
          buffer[i] = '#';
        } else {
          buffer[i] = (byte) ('0' + (i % 10));
        }
      }
      // new Random().nextBytes(buffer);
      byte[] byteID = ids.getByteID();
      VLogEntryInfo info;

      Monitor m = MeasureFactory.start("write");
      try {
        info = vLogFile.put(FAMILY, byteID, 1, buffer);
      } finally {
        m.stop();
      }

      System.out.println(info.toString());

      byteID = ids.getByteID();
      m = MeasureFactory.start("write");
      try {
        info = vLogFile.put(FAMILY, byteID, 1, buffer);
      } finally {
        m.stop();
      }

      System.out.println(info.toString());

      testFileBin(vLogFile, buffer, byteID, info);
    }
    System.out.println(MeasureFactory.asString());
  }

  @Order(2)
  @Test
  void test1000Bin() throws IOException, InterruptedException, NoSuchAlgorithmException {
    System.out.println("test 1000 bin");
    deleteFolder();
    List<byte[]> descs = new ArrayList<>();

    Map<byte[], VLogEntryInfo> infos = new HashMap<>();
    VLogEntryInfo info = null;
    try (VLogFile vLogFile = new VLogFile(options, 2)) {
      byte[] buffer = new byte[1024 * 1024];
      new Random().nextBytes(buffer);
      for (int i = 1; i <= MAX_DOCS; i++) {
        byte[] id = ids.getByteID();
        descs.add(id);
        String idStr = UUID.nameUUIDFromBytes(id).toString();
        Monitor m = MeasureFactory.start("write");
        try {
          info = vLogFile.put(FAMILY, id, 1, buffer);
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

        testFileBin(vLogFile, buffer, id, infos.get(id));
      }
    }

    System.out.println("test iterator");

    List<String> myIds = new ArrayList<>();
    Map<String, VLogEntryInfo> myMap = new HashMap<>();
    descs.forEach(k -> {
      myIds.add(ByteArrayUtils.bytesAsHexString(k));
      VLogEntryInfo vLogEntryInfo = infos.get(k);
      myMap.put(ByteArrayUtils.bytesAsHexString(k), vLogEntryInfo);
    });

    int count = 0;
    try (VLogFile vLogFile = new VLogFile(options, 2)) {
      List<VLogEntryDescription> list = new ArrayList<>();
      for (Iterator<VLogEntryDescription> iterator = vLogFile.iterator(); iterator.hasNext();) {
        VLogEntryDescription type = iterator.next();
        System.out.println(type.toJsonString());
        list.add(type);
        String key = ByteArrayUtils.bytesAsHexString(type.getKey());
        if (myIds.contains(key)) {
          count++;
          myIds.remove(key);
        }
        VLogEntryInfo vLogEntryInfo = myMap.get(key);
        assertNotNull(vLogEntryInfo);
        assertEquals(vLogEntryInfo.end, type.end);
        assertTrue(Arrays.equals(vLogEntryInfo.hash, type.hash));
        assertEquals(vLogEntryInfo.start, type.start);
        assertEquals(vLogEntryInfo.startBinary, type.startBinary);
        assertEquals(1, type.chunkNumber);
        assertEquals(vLogFile.getName(), type.containerName);
        assertEquals("EASY", type.family);
        assertEquals(vLogEntryInfo.getBinarySize(), type.length);
      }
      assertEquals(MAX_DOCS, list.size());
      assertEquals(0, myIds.size());
    }

    System.out.printf("error on id: %d\r\n", ids.getErrorCount());
    System.out.println(MeasureFactory.asString());
  }

  private void testFileBin(VLogFile vLogFile, byte[] buffer, byte[] byteId, VLogEntryInfo info) throws IOException {
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

    assertTrue(Arrays.equals(buffer, out.toByteArray()));

    out.reset();
    m = MeasureFactory.start("readDescr");
    try {
      try (InputStream input = vLogFile.get(info.start, info.getDescriptionSize())) {
        assertNotNull(input);
        IOUtils.copy(input, out);
      }
    } finally {
      m.stop();
    }

    VLogDescriptor descriptor = VLogDescriptor.fromBytes(out.toByteArray());
    assertEquals(info.getBinarySize(), descriptor.length);
    assertEquals(buffer.length, descriptor.length);
    assertEquals(ByteArrayUtils.bytesAsHexString(info.hash), ByteArrayUtils.bytesAsHexString(descriptor.hash));
    assertEquals(ByteArrayUtils.bytesAsHexString(byteId), ByteArrayUtils.bytesAsHexString(descriptor.key));
    assertEquals(FAMILY, new String(descriptor.familyBytes, StandardCharsets.UTF_8));
    assertEquals(1, descriptor.chunkNumber);
  }

  @Order(3)
  @Test
  public void testIterator() throws IOException {
  }
}
