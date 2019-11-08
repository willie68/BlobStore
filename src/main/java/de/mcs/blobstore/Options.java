/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: BlobStore
 * File: Options.java
 * EMail: W.Klaas@gmx.de
 * Created: 07.11.2019 wklaa_000
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

/**
 * @author wklaa_000
 *
 */
public class Options {

  /**
   * how much is the delete treshhold 
   * if the deleted data bytes are > then the treshhold (in percent) than the container will be compacted.
   */
  int vCntDeleteTreshHold;

  /**
   * the age of the container after that this container will be compressed. in days. 0= no compression
   */
  long vCntCompressAge;

  /**
   * mode of the compression
   */
  int vCntCompressionMode;

  /**
   * maximum size of a vLog file. If the file is > than that, the vLog file will be marked for compacting and as read only
   */
  long vlogMaxSize;

  /**
   * maximum count of docs of a vLog file. If the count is > than that, the vLog file will be marked for compacting and as read only
   */
  long vlogMaxCount;

  /**
   * if the last write access is oldeer than that, the vLog file will be marked for compacting and as read only
   */
  long vLogAge;

  /**
   * @return the vCntDeleteTreshHold
   */
  public int getvCntDeleteTreshHold() {
    return vCntDeleteTreshHold;
  }

  /**
   * @param vCntDeleteTreshHold the vCntDeleteTreshHold to set
   * @return 
   */
  public Options setvCntDeleteTreshHold(int vCntDeleteTreshHold) {
    this.vCntDeleteTreshHold = vCntDeleteTreshHold;
    return this;
  }

  /**
   * @return the vCntCompressAge
   */
  public long getvCntCompressAge() {
    return vCntCompressAge;
  }

  /**
   * @param vCntCompressAge the vCntCompressAge to set
   */
  public Options setvCntCompressAge(long vCntCompressAge) {
    this.vCntCompressAge = vCntCompressAge;
    return this;
  }

  /**
   * @return the vCntCompressionMode
   */
  public int getvCntCompressionMode() {
    return vCntCompressionMode;
  }

  /**
   * @param vCntCompressionMode the vCntCompressionMode to set
   */
  public Options setvCntCompressionMode(int vCntCompressionMode) {
    this.vCntCompressionMode = vCntCompressionMode;
    return this;
  }

  /**
   * @return the vlogMaxSize
   */
  public long getVlogMaxSize() {
    return vlogMaxSize;
  }

  /**
   * @param vlogMaxSize the vlogMaxSize to set
   */
  public Options setVlogMaxSize(long vlogMaxSize) {
    this.vlogMaxSize = vlogMaxSize;
    return this;
  }

  /**
   * @return the vlogMaxCount
   */
  public long getVlogMaxCount() {
    return vlogMaxCount;
  }

  /**
   * @param vlogMaxCount the vlogMaxCount to set
   */
  public Options setVlogMaxCount(long vlogMaxCount) {
    this.vlogMaxCount = vlogMaxCount;
    return this;
  }

  /**
   * @return the vLogAge
   */
  public long getvLogAge() {
    return vLogAge;
  }

  /**
   * @param vLogAge the vLogAge to set
   */
  public Options setvLogAge(long vLogAge) {
    this.vLogAge = vLogAge;
    return this;
  }
}
