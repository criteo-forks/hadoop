/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class returns build information about Hadoop components.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);

  private Properties info;

  protected VersionInfo(String component) {
    info = new Properties();
    String versionInfoFile = component + "-version-info.properties";
    InputStream is = null;
    try {
      is = ThreadUtil.getResourceAsStream(VersionInfo.class.getClassLoader(),
          versionInfoFile);
      info.load(is);
    } catch (IOException ex) {
      LoggerFactory.getLogger(getClass()).warn("Could not read '" +
          versionInfoFile + "', " + ex.toString(), ex);
    } finally {
      IOUtils.closeStream(is);
    }
  }

  protected String _getVersion() {
    return info.getProperty("version", "Unknown");
  }

  protected String _getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  protected String _getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  protected String _getDate() {
    return info.getProperty("date", "Unknown");
  }

  protected String _getUser() {
    return info.getProperty("user", "Unknown");
  }

  protected String _getUrl() {
    return info.getProperty("url", "Unknown");
  }

  protected String _getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected String _getBuildVersion(){
    return _getVersion() +
      " from " + _getRevision() +
      " by " + _getUser() +
      " source checksum " + _getSrcChecksum();
  }

  protected String _getProtocVersion() {
    return info.getProperty("protocVersion", "Unknown");
  }

  private static VersionInfo COMMON_VERSION_INFO = new VersionInfo("common");
  /**
   * Get the Hadoop version.
   * @return the Hadoop version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {

    // This is a nasty way to trick spark hive 1.2 fork. This fork uses ShimLoader that does not recognise hadoop 3 version.
    // If all user code was using our spark fork, we could manage this at this level and it would be cleaner.
    // But that is not the case, some jobs embed spark thus we have no control on the embedded hive classes.
    //
    // The option left we have is to detect that this method is called from hive ShimLoader and return a fake version 2 String
    //
    // Proper fix options are:
    //   - force everybody to use our spark fork
    //   - when it is absolutely not possible, force them to use our own fork of spark hive fork
    //   - migrate all spark jobs to spark 3

    StackTraceElement[] els =  Thread.currentThread().getStackTrace();
    if (els.length >= 3) {
      StackTraceElement el = els[2];
      String caller = el.getClassName() + "." + el.getMethodName();

      if (caller.equals("org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion")) {
        return "2.X.Y-fake-for-spark-hive";
      }
    }

    return COMMON_VERSION_INFO._getVersion();
  }
  
  /**
   * Get the Git commit hash of the repository when compiled.
   * @return the commit hash, eg. "18f64065d5db6208daf50b02c1b5ed4ee3ce547a"
   */
  public static String getRevision() {
    return COMMON_VERSION_INFO._getRevision();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  public static String getBranch() {
    return COMMON_VERSION_INFO._getBranch();
  }

  /**
   * The date that Hadoop was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return COMMON_VERSION_INFO._getDate();
  }

  /**
   * The user that compiled Hadoop.
   * @return the username of the user
   */
  public static String getUser() {
    return COMMON_VERSION_INFO._getUser();
  }

  /**
   * Get the URL for the Hadoop repository.
   * @return the URL of the Hadoop repository
   */
  public static String getUrl() {
    return COMMON_VERSION_INFO._getUrl();
  }

  /**
   * Get the checksum of the source files from which Hadoop was built.
   * @return the checksum of the source files
   */
  public static String getSrcChecksum() {
    return COMMON_VERSION_INFO._getSrcChecksum();
  }

  /**
   * Returns the buildVersion which includes version,
   * revision, user and date.
   * @return the buildVersion
   */
  public static String getBuildVersion(){
    return COMMON_VERSION_INFO._getBuildVersion();
  }

  /**
   * Returns the protoc version used for the build.
   * @return the protoc version
   */
  public static String getProtocVersion(){
    return COMMON_VERSION_INFO._getProtocVersion();
  }

  public static void main(String[] args) {
    LOG.debug("version: "+ getVersion());
    System.out.println("Hadoop " + getVersion());
    System.out.println("Source code repository " + getUrl() + " -r " +
        getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("Compiled with protoc " + getProtocVersion());
    System.out.println("From source with checksum " + getSrcChecksum());
    System.out.println("This command was run using " + 
        ClassUtil.findContainingJar(VersionInfo.class));
  }
}
