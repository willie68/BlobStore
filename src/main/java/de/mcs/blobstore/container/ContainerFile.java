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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.input.BoundedInputStream;

import de.mcs.blobstore.BlobsDBException;
import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.Options;
import de.mcs.blobstore.vlog.VLogDescriptor;
import de.mcs.blobstore.vlog.VLogEntryDescription;
import de.mcs.blobstore.vlog.VLogEntryInfo;
import de.mcs.blobstore.vlog.VLogFile;
import de.mcs.utils.HashUtils;
import de.mcs.utils.io.RandomAccessInputStream;
import de.mcs.utils.logging.Logger;

public class ContainerFile {

  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File contFile;
  private File jsonFile;
  private FileChannel fileChannel;
  private RandomAccessFile writer;
  private Options options;
  private int chunkCount;
  private boolean readOnly;

  public static File getFilePathName(File path, int number) {
    String internalName = String.format("cont_%04d.cont", number);
    return new File(path, internalName);
  }

  public static boolean isContainer(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".cont");
  }

  private ContainerFile() {
    chunkCount = -1;
  }

  public ContainerFile(Options options, int number) throws IOException {
    this();
    this.options = options;
    this.contFile = getFilePathName(new File(options.getPath()), number);
    init();
  }

  public ContainerFile(Options options, File file) throws IOException {
    this();
    this.options = options;
    this.contFile = file;
    init();
  }

  private void init() throws IOException {
    internalName = contFile.getName();
    if (contFile.exists()) {
      loadLogFile();
    } else {
      initLogFile();
    }
  }

  private void loadLogFile() throws IOException {
    log.debug("loading vlog file: %s", internalName);
    writer = new RandomAccessFile(contFile, "r");
    writer.seek(writer.length());
    fileChannel = writer.getChannel();
    chunkCount = -1;
    readOnly = true;
  }

  private void initLogFile() throws FileNotFoundException {
    log.debug("creating new vlog file: %s", internalName);
    writer = new RandomAccessFile(contFile, "rw");
    fileChannel = writer.getChannel();
    chunkCount = 0;
    readOnly = false;
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
    writer.close();
  }

  public VLogEntryInfo put(String family, byte[] key, int chunknumber, byte[] chunk) throws IOException {
    byte[] familyBytes = family.getBytes(StandardCharsets.UTF_8);
    if (familyBytes.length > VLogDescriptor.KEY_MAX_LENGTH) {
      throw new BlobsDBException("Illegal family length.");
    }
    if (key.length > VLogDescriptor.KEY_MAX_LENGTH) {
      throw new BlobsDBException("Illegal key length.");
    }
    if (!isAvailbleForWriting()) {
      throw new BlobsDBException(String.format("VLogfile %s is not availble for writing.", internalName));
    }
    // calculating hash of chunk
    ByteArrayInputStream in = new ByteArrayInputStream(chunk);
    byte[] digest = HashUtils.hash(messageDigest, in);
    in.reset();

    VLogEntryInfo info = new VLogEntryInfo();
    info.start = fileChannel.position();
    info.hash = digest;

    VLogDescriptor vlogDescriptor = new VLogDescriptor();
    vlogDescriptor.familyBytes = familyBytes;
    vlogDescriptor.key = key;
    vlogDescriptor.chunkNumber = chunknumber;
    vlogDescriptor.hash = digest;
    vlogDescriptor.length = chunk.length;
    fileChannel.write(vlogDescriptor.getBytes());

    info.startBinary = fileChannel.position();

    // write the binary data
    fileChannel.write(ByteBuffer.wrap(chunk));

    info.end = fileChannel.position() - 1;
    fileChannel.force(true);
    chunkCount++;
    return info;
  }

  public InputStream get(long offset, long size) throws IOException {
    return new BufferedInputStream(new BoundedInputStream(new RandomAccessInputStream(contFile, offset), size),
        options.getVlogChunkSize());
  }

  public long getSize() {
    return contFile.length();
  }

  public boolean isAvailbleForWriting() {
    if (readOnly) {
      return false;
    }
    if (getSize() > options.getVlogMaxSize()) {
      return false;
    }
    if (getChunkCount() > options.getVlogMaxChunkCount()) {
      return false;
    }
    return true;
  }

  /**
   * @return the chunkCount
   */
  public int getChunkCount() {
    return chunkCount;
  }

  /**
   * @return the readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  public VLogFile setReadOnly(boolean readonly) {
    this.readOnly = readonly;
    return this;
  }

  public File getFile() {
    return contFile;
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
          if (!Arrays.equals(VLogDescriptor.DOC_START, next)) {
            markerFound = false;
          } else {
            long startDescription = position;
            byte[] descriptorArray = input.readNBytes(VLogDescriptor.lengthWithoutStart());
            if (descriptorArray.length != VLogDescriptor.lengthWithoutStart()) {
              throw new IOException("error reading description.");
            }
            position += descriptorArray.length;
            VLogDescriptor descriptor = VLogDescriptor.fromBytesWithoutStart(descriptorArray);
            if (descriptor == null) {
              throw new IOException("length not ok");
            } else {
              // System.out.println("entry found: \r\n");
              VLogEntryDescription info = new VLogEntryDescription();
              info.chunkNumber = descriptor.chunkNumber;
              info.containerName = getName();
              info.end = position + descriptor.length - 1;
              info.family = new String(descriptor.familyBytes, StandardCharsets.UTF_8);
              info.hash = descriptor.hash;
              info.key = descriptor.key;
              info.length = descriptor.length;
              info.start = start;
              info.startBinary = position;
              info.startDescription = startDescription;
              long bytesToSkip = descriptor.length;
              while ((bytesToSkip > 0) && (input.available() > 0)) {
                long skip = input.skip(bytesToSkip);
                if (skip < 0) {
                  throw new IOException("vLog not correctly padded.");
                }
                bytesToSkip -= skip;
                position += skip;
              }
              entryInfos.add(info);
            }
          }
          if (!markerFound) {
          }
        }
      }
    }
    return entryInfos.iterator();
  }

}
