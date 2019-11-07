/**
 * MCS Media Computer Software
 * Copyright 2019 by Wilfried Klaas
 * Project: MCSUtils
 * File: Logger.java
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
package de.mcs.utils.logging;

import java.util.Enumeration;
import java.util.ResourceBundle;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

/**
 * @author wklaa_000
 *
 */
public class Logger {

  static public Logger getLogger(Class clazz) {
    return new Logger(LogManager.getLogger(clazz.getName()));
  }

  private org.apache.log4j.Logger log;

  protected Logger(String name) {
    this.log = org.apache.log4j.Logger.getLogger(name);
  }

  public Logger(org.apache.log4j.Logger logger) {
    this.log = logger;
  }

  public void debug(String format, Object... objects) {
    log.debug(String.format(format, objects));
  }

  /**
   * @return
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return log.hashCode();
  }

  /**
   * @param obj
   * @return
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj) {
    return log.equals(obj);
  }

  /**
   * @param message
   * @see org.apache.log4j.Logger#trace(java.lang.Object)
   */
  public void trace(Object message) {
    log.trace(message);
  }

  /**
   * @param newAppender
   * @see org.apache.log4j.Category#addAppender(org.apache.log4j.Appender)
   */
  public void addAppender(Appender newAppender) {
    log.addAppender(newAppender);
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Logger#trace(java.lang.Object, java.lang.Throwable)
   */
  public void trace(Object message, Throwable t) {
    log.trace(message, t);
  }

  /**
   * @param assertion
   * @param msg
   * @see org.apache.log4j.Category#assertLog(boolean, java.lang.String)
   */
  public void assertLog(boolean assertion, String msg) {
    log.assertLog(assertion, msg);
  }

  /**
   * @return
   * @see org.apache.log4j.Logger#isTraceEnabled()
   */
  public boolean isTraceEnabled() {
    return log.isTraceEnabled();
  }

  /**
   * @param event
   * @see org.apache.log4j.Category#callAppenders(org.apache.log4j.spi.LoggingEvent)
   */
  public void callAppenders(LoggingEvent event) {
    log.callAppenders(event);
  }

  /**
   * @param message
   * @see org.apache.log4j.Category#debug(java.lang.Object)
   */
  public void debug(Object message) {
    log.debug(message);
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Category#debug(java.lang.Object, java.lang.Throwable)
   */
  public void debug(Object message, Throwable t) {
    log.debug(message, t);
  }

  /**
   * @param message
   * @see org.apache.log4j.Category#error(java.lang.Object)
   */
  public void error(Object message) {
    log.error(message);
  }

  /**
   * @return
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return log.toString();
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Category#error(java.lang.Object, java.lang.Throwable)
   */
  public void error(Object message, Throwable t) {
    log.error(message, t);
  }

  /**
   * @param message
   * @see org.apache.log4j.Category#fatal(java.lang.Object)
   */
  public void fatal(Object message) {
    log.fatal(message);
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Category#fatal(java.lang.Object, java.lang.Throwable)
   */
  public void fatal(Object message, Throwable t) {
    log.fatal(message, t);
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getAdditivity()
   */
  public boolean getAdditivity() {
    return log.getAdditivity();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getAllAppenders()
   */
  public Enumeration getAllAppenders() {
    return log.getAllAppenders();
  }

  /**
   * @param name
   * @return
   * @see org.apache.log4j.Category#getAppender(java.lang.String)
   */
  public Appender getAppender(String name) {
    return log.getAppender(name);
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getEffectiveLevel()
   */
  public Level getEffectiveLevel() {
    return log.getEffectiveLevel();
  }

  /**
   * @return
   * @deprecated
   * @see org.apache.log4j.Category#getChainedPriority()
   */
  public Priority getChainedPriority() {
    return log.getChainedPriority();
  }

  /**
   * @return
   * @deprecated
   * @see org.apache.log4j.Category#getHierarchy()
   */
  public LoggerRepository getHierarchy() {
    return log.getHierarchy();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getLoggerRepository()
   */
  public LoggerRepository getLoggerRepository() {
    return log.getLoggerRepository();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getName()
   */
  public final String getName() {
    return log.getName();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getParent()
   */
  public final Category getParent() {
    return log.getParent();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getLevel()
   */
  public final Level getLevel() {
    return log.getLevel();
  }

  /**
   * @return
   * @deprecated
   * @see org.apache.log4j.Category#getPriority()
   */
  public final Level getPriority() {
    return log.getPriority();
  }

  /**
   * @return
   * @see org.apache.log4j.Category#getResourceBundle()
   */
  public ResourceBundle getResourceBundle() {
    return log.getResourceBundle();
  }

  /**
   * @param message
   * @see org.apache.log4j.Category#info(java.lang.Object)
   */
  public void info(Object message) {
    log.info(message);
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Category#info(java.lang.Object, java.lang.Throwable)
   */
  public void info(Object message, Throwable t) {
    log.info(message, t);
  }

  /**
   * @param appender
   * @return
   * @see org.apache.log4j.Category#isAttached(org.apache.log4j.Appender)
   */
  public boolean isAttached(Appender appender) {
    return log.isAttached(appender);
  }

  /**
   * @return
   * @see org.apache.log4j.Category#isDebugEnabled()
   */
  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  /**
   * @param level
   * @return
   * @see org.apache.log4j.Category#isEnabledFor(org.apache.log4j.Priority)
   */
  public boolean isEnabledFor(Priority level) {
    return log.isEnabledFor(level);
  }

  /**
   * @return
   * @see org.apache.log4j.Category#isInfoEnabled()
   */
  public boolean isInfoEnabled() {
    return log.isInfoEnabled();
  }

  /**
   * @param priority
   * @param key
   * @param t
   * @see org.apache.log4j.Category#l7dlog(org.apache.log4j.Priority, java.lang.String, java.lang.Throwable)
   */
  public void l7dlog(Priority priority, String key, Throwable t) {
    log.l7dlog(priority, key, t);
  }

  /**
   * @param priority
   * @param key
   * @param params
   * @param t
   * @see org.apache.log4j.Category#l7dlog(org.apache.log4j.Priority, java.lang.String, java.lang.Object[], java.lang.Throwable)
   */
  public void l7dlog(Priority priority, String key, Object[] params, Throwable t) {
    log.l7dlog(priority, key, params, t);
  }

  /**
   * @param priority
   * @param message
   * @param t
   * @see org.apache.log4j.Category#log(org.apache.log4j.Priority, java.lang.Object, java.lang.Throwable)
   */
  public void log(Priority priority, Object message, Throwable t) {
    log.log(priority, message, t);
  }

  /**
   * @param priority
   * @param message
   * @see org.apache.log4j.Category#log(org.apache.log4j.Priority, java.lang.Object)
   */
  public void log(Priority priority, Object message) {
    log.log(priority, message);
  }

  /**
   * @param callerFQCN
   * @param level
   * @param message
   * @param t
   * @see org.apache.log4j.Category#log(java.lang.String, org.apache.log4j.Priority, java.lang.Object, java.lang.Throwable)
   */
  public void log(String callerFQCN, Priority level, Object message, Throwable t) {
    log.log(callerFQCN, level, message, t);
  }

  /**
   * 
   * @see org.apache.log4j.Category#removeAllAppenders()
   */
  public void removeAllAppenders() {
    log.removeAllAppenders();
  }

  /**
   * @param appender
   * @see org.apache.log4j.Category#removeAppender(org.apache.log4j.Appender)
   */
  public void removeAppender(Appender appender) {
    log.removeAppender(appender);
  }

  /**
   * @param name
   * @see org.apache.log4j.Category#removeAppender(java.lang.String)
   */
  public void removeAppender(String name) {
    log.removeAppender(name);
  }

  /**
   * @param additive
   * @see org.apache.log4j.Category#setAdditivity(boolean)
   */
  public void setAdditivity(boolean additive) {
    log.setAdditivity(additive);
  }

  /**
   * @param level
   * @see org.apache.log4j.Category#setLevel(org.apache.log4j.Level)
   */
  public void setLevel(Level level) {
    log.setLevel(level);
  }

  /**
   * @param priority
   * @deprecated
   * @see org.apache.log4j.Category#setPriority(org.apache.log4j.Priority)
   */
  public void setPriority(Priority priority) {
    log.setPriority(priority);
  }

  /**
   * @param bundle
   * @see org.apache.log4j.Category#setResourceBundle(java.util.ResourceBundle)
   */
  public void setResourceBundle(ResourceBundle bundle) {
    log.setResourceBundle(bundle);
  }

  /**
   * @param message
   * @see org.apache.log4j.Category#warn(java.lang.Object)
   */
  public void warn(Object message) {
    log.warn(message);
  }

  /**
   * @param message
   * @param t
   * @see org.apache.log4j.Category#warn(java.lang.Object, java.lang.Throwable)
   */
  public void warn(Object message, Throwable t) {
    log.warn(message, t);
  }
}
