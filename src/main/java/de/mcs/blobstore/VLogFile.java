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
package de.mcs.blobstore;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import de.mcs.utils.logging.Logger;

/**
 * This is the implementing class for a value log file, all binary datas will be stored simply in sequential order. 
 * And there are as many v log files open as parallel tasks writing to them.
 * @author wklaa_000
 *
 */
public class VLogFile implements Closeable {

  private Logger log = Logger.getLogger(this.getClass());
  private String internalName;
  private File vLogFile;
  private FileChannel fileChannel;

  public VLogFile(File path, int number) throws IOException {
    internalName = String.format("vlog_%4d.vlog", number);
    vLogFile = new File(path, internalName);
    if (vLogFile.exists()) {
      loadLogFile();
    } else {
      initLogFile();
    }
  }

  private void loadLogFile() throws IOException {
    log.debug("creating new vlog file: %s", internalName);
    fileChannel = new FileOutputStream(vLogFile, true).getChannel();
  }

  private void initLogFile() throws FileNotFoundException {
    log.debug("creating new vlog file: %s", internalName);
    fileChannel = new FileOutputStream(vLogFile).getChannel();
  }

  public String getName() {
    return internalName;
  }

  @Override
  public void close() throws IOException {
    if ((fileChannel != null) && fileChannel.isOpen()) {
      fileChannel.close();
    }
  }

  long put(VLogDescriptor vlogDesc, InputStream in) {

  }
}
