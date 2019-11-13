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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.input.BoundedInputStream;

import de.mcs.blobstore.BlobsDBException;
import de.mcs.blobstore.Options;
import de.mcs.utils.ChannelTools;
import de.mcs.utils.HasherUtils;
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

  private static final String VLOG_VERSION = "1";
  private static final byte[] DOC_START = ("@@@" + VLOG_VERSION).getBytes(StandardCharsets.UTF_8);
  private static final byte[] DOC_LIMITER = "#".getBytes(StandardCharsets.UTF_8);
  private static final int KEY_MAX_LENGTH = 256;
  // because of the headerstructure, 4 bytes DOC_START + 1 byte KEY_LENGTH + KEY
  // itself + 4 bytes Chunknumber + 1 byte DOC_LIMITER
  private static final int HEADER_MAX_LENGTH = DOC_START.length + 1 + KEY_MAX_LENGTH + 4 + DOC_LIMITER.length;
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

  public VLogEntryInfo put(byte[] key, int chunknumber, InputStream in) throws IOException {
    if (key.length > KEY_MAX_LENGTH) {
      throw new BlobsDBException("Illegal key length.");
    }
    if (isReadOnly()) {
      throw new BlobsDBException(String.format("VLogfile %s is read only.", internalName));
    }
    byte keyLength = (byte) key.length;
    VLogEntryInfo info = new VLogEntryInfo();
    info.start = fileChannel.position();
    ByteBuffer header = ByteBuffer.allocateDirect(HEADER_MAX_LENGTH);

    header.rewind();
    header.put(DOC_START);
    header.put(keyLength);
    header.put(key);
    header.putInt(chunknumber);
    header.put(DOC_LIMITER);
    header.flip();
    fileChannel.write(header);
    info.startBinary = fileChannel.position();

    // write the binary data
    MessageDigest messageDigest = HasherUtils.Algorithm.SHA_256.getMessageDigest();
    DigestInputStream digestInputStream = new DigestInputStream(in, messageDigest);
    ReadableByteChannel inChannel = Channels.newChannel(digestInputStream);
    ChannelTools.fastChannelCopy(inChannel, fileChannel);

    byte[] digest = messageDigest.digest();
    info.startPostfix = fileChannel.position();
    info.hash = digest;
    // write the postfix
    VLogPostFix postFix = new VLogPostFix();
    postFix.length = info.startPostfix - info.startBinary;
    postFix.hash = digest;

    fileChannel.write(postFix.getBytes());

    info.end = fileChannel.position();
    fileChannel.force(true);
    return info;
  }

  public InputStream get(long offset, long size) throws IOException {
    return new BoundedInputStream(new RandomAccessInputStream(vLogFile, offset), size);
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

  public Iterator<VLogEntryInfo> getIterator() throws IOException {
    List<VLogEntryInfo> entryInfos = new ArrayList<>();
    RandomAccessInputStream input = new RandomAccessInputStream(vLogFile);
    boolean markerFound = false;
    long position = 0;
    while (input.available() > 0) {
      markerFound = true;
      byte[] next = input.readNBytes(4);
      if (next.length != 4) {
        markerFound = false;
      }
      if (!Arrays.equals(DOC_START, next)) {
        markerFound = false;
      }

      if (!markerFound) {
        input.position(position + 1);
      }
    }
    return entryInfos.iterator();
  }
}
