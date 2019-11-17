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
package de.mcs.blobstore.container;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.input.BoundedInputStream;

import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.ContainerReader;
import de.mcs.blobstore.Options;
import de.mcs.blobstore.vlog.VLogEntryDescription;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.utils.GsonUtils;
import de.mcs.utils.io.RandomAccessInputStream;
import de.mcs.utils.logging.Logger;

public class ContainerFile implements Closeable, ContainerReader {

  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File contFile;
  private File jsonFile;
  private FileChannel fileChannel;
  private RandomAccessFile raf;
  private Options options;
  private ContainerProperties containerProperties;

  public static File getJsonFilePathName(File path, int number) {
    String internalName = String.format("cont_%04d.json", number);
    return new File(path, internalName);
  }

  public static File getFilePathName(File path, int number) {
    String internalName = String.format("cont_%04d.cont", number);
    return new File(path, internalName);
  }

  public static boolean isContainer(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".cont");
  }

  private ContainerFile() {
  }

  public ContainerFile(Options options, String family, int number) throws IOException {
    this();
    this.options = options;
    File path = new File(options.getPath(), family);
    if (!path.exists()) {
      path.mkdirs();
    }
    this.contFile = getFilePathName(path, number);
    this.jsonFile = getJsonFilePathName(path, number);
    init();
  }

  private void init() throws IOException {
    internalName = contFile.getName();
    loadJsonFile();

    if (containerProperties.isReadOnly()) {
      loadContFile();
    } else {
      initContFile();
    }
  }

  private void loadJsonFile() {
    try {
      if (jsonFile.exists()) {
        Reader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(jsonFile)));
        this.containerProperties = GsonUtils.getJsonMapper().fromJson(reader, ContainerProperties.class);
      } else {
        this.containerProperties = new ContainerProperties().setName(getName()).setReadOnly(false).setChunkCount(0);
      }
    } catch (FileNotFoundException e) {
      // should never occure
      log.error(e);
    }
  }

  private void saveJsonFile() {
    try {
      Writer writer = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(jsonFile)));
      GsonUtils.getJsonMapper().toJson(this.containerProperties, writer);
    } catch (FileNotFoundException e) {
      // should never occure
      log.error(e);
    }
  }

  private void loadContFile() throws IOException {
    log.debug("loading container file for reading: %s", internalName);
    raf = new RandomAccessFile(contFile, "r");
    fileChannel = raf.getChannel();
  }

  private void initContFile() throws FileNotFoundException {
    log.debug("open container file for writing: %s", internalName);
    raf = new RandomAccessFile(contFile, "rw");
    fileChannel = raf.getChannel();
  }

  public String getName() {
    return internalName;
  }

  @Override
  public void close() throws IOException {
    if ((fileChannel != null) && fileChannel.isOpen()) {
      fileChannel.force(true);
      fileChannel.close();
    }
    raf.close();
  }

  public VLogEntryInfo put(String family, byte[] key, int chunknumber, byte[] chunk) throws IOException {
    // byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
    // if (familyBytes.length > VLogDescriptor.KEY_MAX_LENGTH) {
    // throw new BlobsDBException("Illegal family length.");
    // }
    // if (key.length > VLogDescriptor.KEY_MAX_LENGTH) {
    // throw new BlobsDBException("Illegal key length.");
    // }
    // if (!isAvailbleForWriting()) {
    // throw new BlobsDBException(String.format("VLogfile %s is not availble for
    // writing.", internalName));
    // }
    // // calculating hash of chunk
    // ByteArrayInputStream in = new ByteArrayInputStream(chunk);
    // byte[] digest = HashUtils.hash(messageDigest, in);
    // in.reset();
    //
    VLogEntryInfo info = new VLogEntryInfo();
    // info.start = fileChannel.position();
    // info.hash = digest;
    //
    // VLogDescriptor vlogDescriptor = new VLogDescriptor();
    // vlogDescriptor.familyBytes = familyBytes;
    // vlogDescriptor.key = key;
    // vlogDescriptor.chunkNumber = chunknumber;
    // vlogDescriptor.hash = digest;
    // vlogDescriptor.length = chunk.length;
    // fileChannel.write(vlogDescriptor.getBytes());
    //
    // info.startBinary = fileChannel.position();
    //
    // // write the binary data
    // fileChannel.write(ByteBuffer.wrap(chunk));
    //
    // info.end = fileChannel.position() - 1;
    // fileChannel.force(true);
    // chunkCount++;
    return info;
  }

  @Override
  public InputStream get(long offset, long size) throws IOException {
    return new BufferedInputStream(new BoundedInputStream(new RandomAccessInputStream(contFile, offset), size),
        options.getVlogChunkSize());
  }

  public long getSize() {
    return contFile.length();
  }

  public boolean isAvailbleForWriting() {
    if (containerProperties.isReadOnly()) {
      return false;
    }
    if (getSize() > options.getvCntMaxSize()) {
      return false;
    }
    if (getChunkCount() > options.getvCntMaxChunkCount()) {
      return false;
    }
    return true;
  }

  /**
   * @return the chunkCount
   */
  public long getChunkCount() {
    return containerProperties.getChunkCount();
  }

  /**
   * @return the readOnly
   */
  public boolean isReadOnly() {
    return containerProperties.isReadOnly();
  }

  public Iterator<VLogEntryDescription> iterator() throws IOException {
    List<VLogEntryDescription> entryInfos = new ArrayList<>();

    try (RandomAccessInputStream in = new RandomAccessInputStream(contFile)) {
      try (BufferedInputStream input = new BufferedInputStream(in)) {
        boolean markerFound = false;
        long position = 0;
        while (input.available() > 0) {
          markerFound = true;
          long start = position;
          byte[] next = input.readNBytes(4);
          if (next.length != 4) {
            markerFound = false;
          }
          position += 4;
          // if (!Arrays.equals(VLogDescriptor.DOC_START, next)) {
          // markerFound = false;
          // } else {
          // long startDescription = position;
          // byte[] descriptorArray =
          // input.readNBytes(VLogDescriptor.lengthWithoutStart());
          // if (descriptorArray.length != VLogDescriptor.lengthWithoutStart())
          // {
          // throw new IOException("error reading description.");
          // }
          // position += descriptorArray.length;
          // VLogDescriptor descriptor =
          // VLogDescriptor.fromBytesWithoutStart(descriptorArray);
          // if (descriptor == null) {
          // throw new IOException("length not ok");
          // } else {
          // // System.out.println("entry found: \r\n");
          // VLogEntryDescription info = new VLogEntryDescription();
          // info.chunkNumber = descriptor.chunkNumber;
          // info.containerName = getName();
          // info.end = position + descriptor.length - 1;
          // info.family = new String(descriptor.familyBytes,
          // StandardCharsets.UTF_8);
          // info.hash = descriptor.hash;
          // info.key = descriptor.key;
          // info.length = descriptor.length;
          // info.start = start;
          // info.startBinary = position;
          // info.startDescription = startDescription;
          // long bytesToSkip = descriptor.length;
          // while ((bytesToSkip > 0) && (input.available() > 0)) {
          // long skip = input.skip(bytesToSkip);
          // if (skip < 0) {
          // throw new IOException("vLog not correctly padded.");
          // }
          // bytesToSkip -= skip;
          // position += skip;
          // }
          // entryInfos.add(info);
          // }
          // }
          // if (!markerFound) {
          // }
        }
      }
    }
    return entryInfos.iterator();
  }

}
