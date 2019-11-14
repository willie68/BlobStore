/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: vlogFile.java
 * EMail: W.Klaas@gmx.de
 * Created: 06.11.2019 wklaa_000
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
package de.mcs.blobstore.vlog;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
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
import de.mcs.utils.HasherUtils;
import de.mcs.utils.HasherUtils.Algorithm;
import de.mcs.utils.io.RandomAccessInputStream;
import de.mcs.utils.logging.Logger;

/**
 * This is the implementing class for a value log file, all binary datas will be stored simply in sequential order.
 * And there are as many v log files open as parallel tasks writing to them.
 * 
 * @author wklaa_000
 *
 */
public class VLogFile implements Closeable {

  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File vLogFile;
  private FileChannel fileChannel;
  private RandomAccessFile writer;
  private Options options;
  private int chunkCount;
  private boolean readOnly;

  public static File getFilePathName(File path, int number) {
    String internalName = String.format("vlog_%04d.vlog", number);
    return new File(path, internalName);
  }

  public static boolean isVLog(ChunkEntry chunk) {
    return chunk.getContainerName().endsWith(".vlog");
  }

  private VLogFile() {
    chunkCount = -1;
  }

  public VLogFile(Options options, int number) throws IOException {
    this();
    this.options = options;
    this.vLogFile = getFilePathName(new File(options.getPath()), number);
    init();
  }

  public VLogFile(Options options, File file) throws IOException {
    this();
    this.options = options;
    this.vLogFile = file;
    init();
  }

  private void init() throws IOException {
    internalName = vLogFile.getName();
    if (vLogFile.exists()) {
      loadLogFile();
    } else {
      initLogFile();
    }
  }

  private void loadLogFile() throws IOException {
    log.debug("loading vlog file: %s", internalName);
    writer = new RandomAccessFile(vLogFile, "r");
    writer.seek(writer.length());
    fileChannel = writer.getChannel();
    chunkCount = -1;
    readOnly = true;
  }

  private void initLogFile() throws FileNotFoundException {
    log.debug("creating new vlog file: %s", internalName);
    writer = new RandomAccessFile(vLogFile, "rw");
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
    if (isReadOnly()) {
      throw new BlobsDBException(String.format("VLogfile %s is read only.", internalName));
    }
    // calculating hash of chunk
    ByteArrayInputStream in = new ByteArrayInputStream(chunk);
    byte[] digest = HasherUtils.hash(Algorithm.SHA_256.getMessageDigest(), in);
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

    return info;
  }

  public InputStream get(long offset, long size) throws IOException {
    return new BufferedInputStream(new BoundedInputStream(new RandomAccessInputStream(vLogFile, offset), size),
        options.getVlogChunkSize());
  }

  public long getSize() {
    return vLogFile.length();
  }

  public boolean isAvailbleForWriting() {
    if (getSize() > options.getVlogMaxSize()) {
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
    return vLogFile;
  }

  public Iterator<VLogEntryDescription> iterator() throws IOException {
    List<VLogEntryDescription> entryInfos = new ArrayList<>();

    try (RandomAccessInputStream in = new RandomAccessInputStream(vLogFile)) {
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
