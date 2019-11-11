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
import java.nio.charset.Charset;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import org.apache.commons.io.input.BoundedInputStream;

import de.mcs.blobstore.BlobException;
import de.mcs.utils.ChannelTools;
import de.mcs.utils.GsonUtils;
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
  private static final int BUFFER_LENGTH = 8096;
  private static final byte[] DOC_START = ("@@@" + VLOG_VERSION).getBytes(Charset.forName("UTF-8"));
  private static final byte[] DOC_LIMITER = "#".getBytes(Charset.forName("UTF-8"));
  private static final int KEY_LENGTH = 16;
  private static final int HEADER_LENGTH = DOC_START.length + KEY_LENGTH + 4 + DOC_LIMITER.length;
  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File vLogFile;
  private FileChannel fileChannel;
  private RandomAccessFile writer;

  public static File getFilePathName(File path, int number) {
    String internalName = String.format("vlog_%04d.vlog", number);
    return new File(path, internalName);
  }

  public VLogFile(File path, int number) throws IOException {
    vLogFile = getFilePathName(path, number);
    init();
  }

  public VLogFile(File file) throws IOException {
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
    log.debug("creating new vlog file: %s", internalName);
    writer = new RandomAccessFile(vLogFile, "rw");
    writer.seek(writer.length());
    fileChannel = writer.getChannel();
  }

  private void initLogFile() throws FileNotFoundException {
    log.debug("creating new vlog file: %s", internalName);
    writer = new RandomAccessFile(vLogFile, "rw");
    fileChannel = writer.getChannel();
  }

  public String getName() {
    return internalName;
  }

  @Override
  public void close() throws IOException {
    fileChannel.force(true);
    if ((fileChannel != null) && fileChannel.isOpen()) {
      fileChannel.close();
    }
    writer.close();
  }

  public VLogEntryInfo put(byte[] key, int chunknumber, InputStream in) throws IOException {
    if (key.length != KEY_LENGTH) {
      throw new BlobException("Illegal key length.");
    }
    VLogEntryInfo info = new VLogEntryInfo();
    info.start = fileChannel.position();
    ByteBuffer header = ByteBuffer.allocateDirect(HEADER_LENGTH);

    header.rewind();
    header.put(DOC_START);
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

  public VLogEntryInfo put(VLogDescriptor vlogDesc, InputStream in) throws IOException {
    VLogEntryInfo info = new VLogEntryInfo();
    info.start = fileChannel.position();
    info.startDescription = info.start + DOC_START.length();

    String json = GsonUtils.getJsonMapper().toJson(vlogDesc);
    int bufSize = DOC_START.length() + json.length();
    ByteBuffer buf = ByteBuffer.allocate(bufSize > BUFFER_LENGTH ? bufSize : BUFFER_LENGTH);
    buf.put(DOC_START.getBytes());
    // first write the description
    buf.put(json.getBytes());
    buf.flip();
    fileChannel.write(buf);
    info.startBinary = fileChannel.position();
    // write the binary data
    MessageDigest messageDigest = HasherUtils.Algorithm.SHA_256.getMessageDigest();
    DigestInputStream digestInputStream = new DigestInputStream(in, messageDigest);
    ReadableByteChannel inChannel = Channels.newChannel(digestInputStream);
    ChannelTools.fastChannelCopy(inChannel, fileChannel);
    info.startPostfix = fileChannel.position();
    byte[] digest = messageDigest.digest();
    info.hash = HasherUtils.bytesAsHexString(digest);
    // write the postfix
    VLogPostFix postFix = new VLogPostFix();
    postFix.length = info.startPostfix - info.startBinary;
    postFix.hash = digest;
    json = GsonUtils.getJsonMapper().toJson(postFix);
    buf.clear();
    buf.put(json.getBytes());
    buf.flip();
    fileChannel.write(buf);
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
    return true;
  }
}
