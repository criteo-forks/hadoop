/**
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

package org.apache.hadoop.tools;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The Options class encapsulates all DistCp options.
 *
 * When you add a new option, please:
 *  - Add the field along with javadoc in DistCpOptions and its Builder
 *  - Add setter method in the {@link Builder} class
 *
 * This class is immutable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class DistCpOptions {
  private static final Logger LOG = LoggerFactory.getLogger(Builder.class);
  public static final int MAX_NUM_LISTSTATUS_THREADS = 40;

  /** File path (hdfs:// or file://) that contains the list of actual files to
   * copy.
   */
  private Path sourceFileListing;

  /** List of source-paths (including wildcards) to be copied to target. */
  private List<Path> sourcePaths;

  /** Destination path for the dist-copy. */
  private Path targetPath;

  /** Whether data need to be committed automatically. */
  private boolean atomicCommit = false;

  /** the work path for atomic commit. If null, the work
   * path would be parentOf(targetPath) + "/._WIP_" + nameOf(targetPath). */
  private Path atomicWorkPath;

  /** Whether source and target folder contents be sync'ed up. */
  private boolean syncFolder = false;

  /** Path to save source/dest sequence files to, if non-null. */
  private Path trackPath;

  /** Whether files only present in target should be deleted. */
  private boolean deleteMissing = false;

  /** Whether failures during copy be ignored. */
  private boolean ignoreFailures = false;

  /** Whether files should always be overwritten on target. */
  private boolean overwrite = false;

  /** Whether we want to append new data to target files. This is valid only
   * with update option and CRC is not skipped. */
  private boolean append = false;

  /** Whether checksum comparison should be skipped while determining if source
   * and destination files are identical. */
  private boolean skipCRC = false;

  /** Whether to run blocking or non-blocking. */
  private boolean blocking = true;

  // When "-diff s1 s2 src tgt" is passed, apply forward snapshot diff (from s1
  // to s2) of source cluster to the target cluster to sync target cluster with
  // the source cluster. Referred to as "Fdiff" in the code.
  // It's required that s2 is newer than s1.
  private boolean useDiff = false;

  // When "-rdiff s2 s1 src tgt" is passed, apply reversed snapshot diff (from
  // s2 to s1) of target cluster to the target cluster, so to make target
  // cluster go back to s1. Referred to as "Rdiff" in the code.
  // It's required that s2 is newer than s1, and src and tgt have exact same
  // content at their s1, if src is not the same as tgt.
  private boolean useRdiff = false;

  /** Whether to log additional info (path, size) in the SKIP/COPY log. */
  private boolean verboseLog = false;

  // For both -diff and -rdiff, given the example command line switches, two
  // steps are taken:
  //   1. Sync Step. This step does renaming/deletion ops in the snapshot diff,
  //      so to avoid copying files copied already but renamed later(HDFS-7535)
  //   2. Copy Step. This step copy the necessary files from src to tgt
  //      2.1 For -diff, it copies from snapshot s2 of src (HDFS-8828)
  //      2.2 For -rdiff, it copies from snapshot s1 of src, where the src
  //          could be the tgt itself (HDFS-9820).
  //

  private String fromSnapshot;
  private String toSnapshot;

  /** The path to a file containing a list of paths to filter out of copy. */
  private String filtersFile;

  /** Path where output logs are stored. If not specified, it will use the
   * default value JobStagingDir/_logs and delete upon job completion. */
  private Path logPath;

  /** Set the copy strategy to use. Should map to a strategy implementation
   * in distp-default.xml. */
  private String copyStrategy = DistCpConstants.UNIFORMSIZE;

  /** per map bandwidth in MB. */
  private float mapBandwidth = DistCpConstants.DEFAULT_BANDWIDTH_MB;

  /** The number of threads to use for listStatus. We allow max
   * {@link #MAX_NUM_LISTSTATUS_THREADS} threads. Setting numThreads to zero
   * signify we should use the value from conf properties. */
  private int numListstatusThreads = 0;

  /** The max number of maps to use for copy. */
  private int maxMaps = DistCpConstants.DEFAULT_MAPS;

  /** File attributes that need to be preserved. */
  private EnumSet<FileAttribute> preserveStatus = EnumSet.noneOf(FileAttribute.class);

  // Size of chunk in number of blocks when splitting large file into chunks
  // to copy in parallel. Default is 0 and file are not splitted.
  private int blocksPerChunk = 0;

  private int copyBufferSize = DistCpConstants.COPY_BUFFER_SIZE_DEFAULT;

  /** Whether data should be written directly to the target paths. */
  private boolean directWrite = false;

  private boolean targetPathExists = true;

  /**
   * File attributes for preserve.
   *
   * Each enum entry uses the first char as its symbol.
   */
  public enum FileAttribute {
    REPLICATION,    // R
    BLOCKSIZE,      // B
    USER,           // U
    GROUP,          // G
    PERMISSION,     // P
    CHECKSUMTYPE,   // C
    ACL,            // A
    XATTR,          // X
    TIMES;          // T

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }

  private DistCpOptions(Builder builder) {
    this.sourceFileListing = builder.sourceFileListing;
    this.sourcePaths = builder.sourcePaths;
    this.targetPath = builder.targetPath;

    this.atomicCommit = builder.atomicCommit;
    this.atomicWorkPath = builder.atomicWorkPath;
    this.syncFolder = builder.syncFolder;
    this.deleteMissing = builder.deleteMissing;
    this.ignoreFailures = builder.ignoreFailures;
    this.overwrite = builder.overwrite;
    this.append = builder.append;
    this.skipCRC = builder.skipCRC;
    this.blocking = builder.blocking;

    this.useDiff = builder.useDiff;
    this.useRdiff = builder.useRdiff;
    this.fromSnapshot = builder.fromSnapshot;
    this.toSnapshot = builder.toSnapshot;

    this.filtersFile = builder.filtersFile;
    this.logPath = builder.logPath;
    this.copyStrategy = builder.copyStrategy;

    this.mapBandwidth = builder.mapBandwidth;
    this.numListstatusThreads = builder.numListstatusThreads;
    this.maxMaps = builder.maxMaps;

    this.preserveStatus = builder.preserveStatus;

    this.blocksPerChunk = builder.blocksPerChunk;

    this.copyBufferSize = builder.copyBufferSize;
    this.verboseLog = builder.verboseLog;
    this.trackPath = builder.trackPath;

    this.directWrite = builder.directWrite;
  }

  public Path getSourceFileListing() {
    return sourceFileListing;
  }

  public List<Path> getSourcePaths() {
    return sourcePaths == null ?
        null : Collections.unmodifiableList(sourcePaths);
  }

  public Path getTargetPath() {
    return targetPath;
  }

  public boolean shouldAtomicCommit() {
    return atomicCommit;
  }

  public Path getAtomicWorkPath() {
    return atomicWorkPath;
  }

  public boolean shouldSyncFolder() {
    return syncFolder;
  }

  public boolean shouldDeleteMissing() {
    return deleteMissing;
  }

  public boolean shouldIgnoreFailures() {
    return ignoreFailures;
  }

  public boolean shouldOverwrite() {
    return overwrite;
  }

  public boolean shouldAppend() {
    return append;
  }

  public boolean shouldSkipCRC() {
    return skipCRC;
  }

  public boolean shouldBlock() {
    return blocking;
  }

  public boolean shouldUseDiff() {
    return this.useDiff;
  }

  public boolean shouldUseRdiff() {
    return this.useRdiff;
  }

  public boolean shouldUseSnapshotDiff() {
    return shouldUseDiff() || shouldUseRdiff();
  }

  public String getFromSnapshot() {
    return this.fromSnapshot;
  }

  public String getToSnapshot() {
    return this.toSnapshot;
  }

  public String getFiltersFile() {
    return filtersFile;
  }

  public Path getLogPath() {
    return logPath;
  }

  public String getCopyStrategy() {
    return copyStrategy;
  }

  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  public int getMaxMaps() {
    return maxMaps;
  }

  public float getMapBandwidth() {
    return mapBandwidth;
  }

  public Set<FileAttribute> getPreserveAttributes() {
    return (preserveStatus == null)
        ? null
        : Collections.unmodifiableSet(preserveStatus);
  }

  /**
   * Checks if the input attribute should be preserved or not.
   *
   * @param attribute - Attribute to check
   * @return True if attribute should be preserved, false otherwise
   */
  public boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }

  public int getBlocksPerChunk() {
    return blocksPerChunk;
  }

  public int getCopyBufferSize() {
    return copyBufferSize;
  }

  public boolean shouldVerboseLog() {
    return verboseLog;
  }

  public Path getTrackPath() {
    return trackPath;
  }

  public boolean shouldDirectWrite() {
    return directWrite;
  }

  /**
   * Add options to configuration. These will be used in the Mapper/committer
   *
   * @param conf - Configuration object to which the options need to be added
   */
  public void appendToConf(Configuration conf) {
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.ATOMIC_COMMIT,
        String.valueOf(atomicCommit));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.IGNORE_FAILURES,
        String.valueOf(ignoreFailures));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.SYNC_FOLDERS,
        String.valueOf(syncFolder));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.DELETE_MISSING,
        String.valueOf(deleteMissing));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.OVERWRITE,
        String.valueOf(overwrite));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.APPEND,
        String.valueOf(append));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.DIFF,
        String.valueOf(useDiff));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.RDIFF,
        String.valueOf(useRdiff));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.SKIP_CRC,
        String.valueOf(skipCRC));
    if (mapBandwidth > 0) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.BANDWIDTH,
          String.valueOf(mapBandwidth));
    }
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));
    if (filtersFile != null) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.FILTERS,
          filtersFile);
    }
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.BLOCKS_PER_CHUNK,
        String.valueOf(blocksPerChunk));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.COPY_BUFFER_SIZE,
        String.valueOf(copyBufferSize));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.VERBOSE_LOG,
        String.valueOf(verboseLog));
    if (trackPath != null) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.TRACK_MISSING,
          String.valueOf(trackPath));
    }
    if (numListstatusThreads > 0) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.NUM_LISTSTATUS_THREADS,
          Integer.toString(numListstatusThreads));
    }
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.DIRECT_WRITE,
            String.valueOf(directWrite));
  }

  /**
   * Utility to easily string-ify Options, for logging.
   *
   * @return String representation of the Options.
   */
  @Override
  public String toString() {
    return "DistCpOptions{" +
        "atomicCommit=" + atomicCommit +
        ", syncFolder=" + syncFolder +
        ", deleteMissing=" + deleteMissing +
        ", ignoreFailures=" + ignoreFailures +
        ", overwrite=" + overwrite +
        ", append=" + append +
        ", useDiff=" + useDiff +
        ", useRdiff=" + useRdiff +
        ", fromSnapshot=" + fromSnapshot +
        ", toSnapshot=" + toSnapshot +
        ", skipCRC=" + skipCRC +
        ", blocking=" + blocking +
        ", numListstatusThreads=" + numListstatusThreads +
        ", maxMaps=" + maxMaps +
        ", mapBandwidth=" + mapBandwidth +
        ", copyStrategy='" + copyStrategy + '\'' +
        ", preserveStatus=" + preserveStatus +
        ", atomicWorkPath=" + atomicWorkPath +
        ", logPath=" + logPath +
        ", sourceFileListing=" + sourceFileListing +
        ", sourcePaths=" + sourcePaths +
        ", targetPath=" + targetPath +
        ", filtersFile='" + filtersFile + '\'' +
        ", blocksPerChunk=" + blocksPerChunk +
        ", copyBufferSize=" + copyBufferSize +
        ", verboseLog=" + verboseLog +
        ", directWrite=" + directWrite +
        '}';
  }

  //restore Distcp mutable style for hadoop migration
  //should only be called by hdp2 client code
  /**
   * Copy constructor.
   * @param that DistCpOptions being copied from.
   */
  public DistCpOptions(DistCpOptions that) {
    if (this != that && that != null) {
      this.atomicCommit = that.atomicCommit;
      this.syncFolder = that.syncFolder;
      this.deleteMissing = that.deleteMissing;
      this.ignoreFailures = that.ignoreFailures;
      this.overwrite = that.overwrite;
      this.skipCRC = that.skipCRC;
      this.blocking = that.blocking;
      this.useDiff = that.useDiff;
      this.useRdiff = that.useRdiff;
      this.numListstatusThreads = that.numListstatusThreads;
      this.maxMaps = that.maxMaps;
      this.mapBandwidth = that.mapBandwidth;
      this.copyStrategy = that.copyStrategy;
      this.preserveStatus = that.preserveStatus;
      this.atomicWorkPath = that.getAtomicWorkPath();
      this.logPath = that.getLogPath();
      this.sourceFileListing = that.getSourceFileListing();
      this.sourcePaths = that.getSourcePaths();
      this.targetPath = that.getTargetPath();
      this.filtersFile = that.getFiltersFile();
    }
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourcePaths List of source-paths (including wildcards)
   *                     to be copied to target.
   * @param targetPath Destination path for the dist-copy.
   */
  public DistCpOptions(List<Path> sourcePaths, Path targetPath) {
    assert sourcePaths != null && !sourcePaths.isEmpty() : "Invalid source paths";
    assert targetPath != null : "Invalid Target path";

    this.sourcePaths = sourcePaths;
    this.targetPath = targetPath;
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourceFileListing File containing list of source paths
   * @param targetPath Destination path for the dist-copy.
   */
  public DistCpOptions(Path sourceFileListing, Path targetPath) {
    assert sourceFileListing != null : "Invalid source paths";
    assert targetPath != null : "Invalid Target path";

    this.sourceFileListing = sourceFileListing;
    this.targetPath = targetPath;
  }

  /**
   * Set if we want to append new data to target files. This is valid only with
   * update option and CRC is not skipped.
   */
  public void setAppend(boolean append) {
    validate(DistCpOptionSwitch.APPEND, append);
    this.append = append;
  }

  /**
   * Set if data need to be committed automatically
   *
   * @param atomicCommit - boolean switch
   */
  public void setAtomicCommit(boolean atomicCommit) {
    validate(DistCpOptionSwitch.ATOMIC_COMMIT, atomicCommit);
    this.atomicCommit = atomicCommit;
  }

  /**
   * Set the work path for atomic commit
   *
   * @param atomicWorkPath - Path on the target cluster
   */
  public void setAtomicWorkPath(Path atomicWorkPath) {
    this.atomicWorkPath = atomicWorkPath;
  }

  /**
   * Set if Disctp should run blocking or non-blocking
   *
   * @param blocking - boolean switch
   */
  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  /**
   * Set the copy strategy to use. Should map to a strategy implementation
   * in distp-default.xml
   *
   * @param copyStrategy - copy Strategy to use
   */
  public void setCopyStrategy(String copyStrategy) {
    this.copyStrategy = copyStrategy;
  }

  /**
   * Set if files only present in target should be deleted
   *
   * @param deleteMissing - boolean switch
   */
  public void setDeleteMissing(boolean deleteMissing) {
    validate(DistCpOptionSwitch.DELETE_MISSING, deleteMissing);
    this.deleteMissing = deleteMissing;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  /**
   * -delete and -diff are mutually exclusive.
   * For backward compatibility, we ignore the -delete option here, instead of
   * throwing an IllegalArgumentException. See HDFS-10397 for more discussion.
   */
  private void ignoreDeleteMissingIfUseSnapshotDiff() {
    if (deleteMissing && (useDiff || useRdiff)) {
      OptionsParser.LOG.warn("-delete and -diff/-rdiff are mutually exclusive. " +
              "The -delete option will be ignored.");
      deleteMissing = false;
    }
  }

  /**
   * Set filtersFile.
   * @param filtersFilename The path to a list of patterns to exclude from copy.
   */
  public final void setFiltersFile(String filtersFilename) {
    this.filtersFile = filtersFilename;
  }

  /**
   * Set if failures during copy be ignored
   *
   * @param ignoreFailures - boolean switch
   */
  public void setIgnoreFailures(boolean ignoreFailures) {
    this.ignoreFailures = ignoreFailures;
  }

  /**
   * Set the log path where distcp output logs are stored
   * Uses JobStagingDir/_logs by default
   *
   * @param logPath - Path where logs will be saved
   */
  public void setLogPath(Path logPath) {
    this.logPath = logPath;
  }

  /**
   * Set per map bandwidth
   *
   * @param mapBandwidth - per map bandwidth
   */
  public void setMapBandwidth(int mapBandwidth) {
    assert mapBandwidth > 0 : "Bandwidth " + mapBandwidth + " is invalid (should be > 0)";
    this.mapBandwidth = mapBandwidth;
  }

  /**
   * Set the max number of maps to use for copy
   *
   * @param maxMaps - Number of maps
   */
  public void setMaxMaps(int maxMaps) {
    this.maxMaps = Math.max(maxMaps, 1);
  }

  /** Set the number of threads to use for listStatus. We allow max 40
   *  threads. Setting numThreads to zero signify we should use the value
   *  from conf properties.
   *
   * @param numThreads - Number of threads
   */
  public void setNumListstatusThreads(int numThreads) {
    if (numThreads > MAX_NUM_LISTSTATUS_THREADS) {
      this.numListstatusThreads = MAX_NUM_LISTSTATUS_THREADS;
    } else if (numThreads > 0) {
      this.numListstatusThreads = numThreads;
    } else {
      this.numListstatusThreads = 0;
    }
  }

  /**
   * Set if files should always be overwritten on target
   *
   * @param overwrite - boolean switch
   */
  public void setOverwrite(boolean overwrite) {
    validate(DistCpOptionSwitch.OVERWRITE, overwrite);
    this.overwrite = overwrite;
  }

  /**
   * Set if checksum comparison should be skipped while determining if
   * source and destination files are identical
   *
   * @param skipCRC - boolean switch
   */
  public void setSkipCRC(boolean skipCRC) {
    validate(DistCpOptionSwitch.SKIP_CRC, skipCRC);
    this.skipCRC = skipCRC;
  }

  /**
   * Setter for sourcePaths.
   * @param sourcePaths The new list of source-paths.
   */
  public void setSourcePaths(List<Path> sourcePaths) {
    assert sourcePaths != null && sourcePaths.size() != 0;
    this.sourcePaths = sourcePaths;
  }

  /**
   * Set if source and target folder contents be sync'ed up
   *
   * @param syncFolder - boolean switch
   */
  public void setSyncFolder(boolean syncFolder) {
    validate(DistCpOptionSwitch.SYNC_FOLDERS, syncFolder);
    this.syncFolder = syncFolder;
  }

  public void setUseDiff(String fromSS, String toSS) {
    this.fromSnapshot = fromSS;
    this.toSnapshot = toSS;
    validate(DistCpOptionSwitch.DIFF, true);
    this.useDiff = true;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  public void setUseRdiff(String fromSS, String toSS) {
    this.fromSnapshot = fromSS;
    this.toSnapshot = toSS;
    validate(DistCpOptionSwitch.RDIFF, true);
    this.useRdiff = true;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  /**
   * Add file attributes that need to be preserved. This method may be
   * called multiple times to add attributes.
   *
   * @param fileAttribute - Attribute to add, one at a time
   */
  public void preserve(FileAttribute fileAttribute) {
    for (FileAttribute attribute : preserveStatus) {
      if (attribute.equals(fileAttribute)) {
        return;
      }
    }
    preserveStatus.add(fileAttribute);
  }

  public void validate(DistCpOptionSwitch option, boolean value) {

    boolean syncFolder = (option == DistCpOptionSwitch.SYNC_FOLDERS ?
            value : this.syncFolder);
    boolean overwrite = (option == DistCpOptionSwitch.OVERWRITE ?
            value : this.overwrite);
    boolean deleteMissing = (option == DistCpOptionSwitch.DELETE_MISSING ?
            value : this.deleteMissing);
    boolean atomicCommit = (option == DistCpOptionSwitch.ATOMIC_COMMIT ?
            value : this.atomicCommit);
    boolean skipCRC = (option == DistCpOptionSwitch.SKIP_CRC ?
            value : this.skipCRC);
    boolean append = (option == DistCpOptionSwitch.APPEND ? value : this.append);
    boolean useDiff = (option == DistCpOptionSwitch.DIFF ? value : this.useDiff);
    boolean useRdiff = (option == DistCpOptionSwitch.RDIFF ? value : this.useRdiff);

    if (syncFolder && atomicCommit) {
      throw new IllegalArgumentException("Atomic commit can't be used with " +
              "sync folder or overwrite options");
    }

    if (deleteMissing && !(overwrite || syncFolder)) {
      throw new IllegalArgumentException("Delete missing is applicable " +
              "only with update or overwrite options");
    }

    if (overwrite && syncFolder) {
      throw new IllegalArgumentException("Overwrite and update options are " +
              "mutually exclusive");
    }

    if (!syncFolder && skipCRC) {
      throw new IllegalArgumentException("Skip CRC is valid only with update options");
    }

    if (!syncFolder && append) {
      throw new IllegalArgumentException(
              "Append is valid only with update options");
    }
    if (skipCRC && append) {
      throw new IllegalArgumentException(
              "Append is disallowed when skipping CRC");
    }
    if (!syncFolder && (useDiff || useRdiff)) {
      throw new IllegalArgumentException(
              "-diff/-rdiff is valid only with -update option");
    }

    if (useDiff || useRdiff) {
      if (StringUtils.isBlank(fromSnapshot) ||
              StringUtils.isBlank(toSnapshot)) {
        throw new IllegalArgumentException(
                "Must provide both the starting and ending " +
                        "snapshot names for -diff/-rdiff");
      }
    }
    if (useDiff && useRdiff) {
      throw new IllegalArgumentException(
              "-diff and -rdiff are mutually exclusive");
    }
  }

  public boolean setTargetPathExists(boolean targetPathExists) {
    return this.targetPathExists = targetPathExists;
  }

  public boolean isTargetPathExists() {
    return targetPathExists;
  }

  /**
   * The builder of the {@link DistCpOptions}.
   *
   * This is designed to be the only public interface to create a
   * {@link DistCpOptions} object for users. It follows a simple Builder design
   * pattern.
   */
  public static class Builder {
    private Path sourceFileListing;
    private List<Path> sourcePaths;
    private Path targetPath;

    private boolean atomicCommit = false;
    private Path atomicWorkPath;
    private boolean syncFolder = false;
    private boolean deleteMissing = false;
    private boolean ignoreFailures = false;
    private boolean overwrite = false;
    private boolean append = false;
    private boolean skipCRC = false;
    private boolean blocking = true;
    private boolean verboseLog = false;

    private boolean useDiff = false;
    private boolean useRdiff = false;
    private String fromSnapshot;
    private String toSnapshot;

    private String filtersFile;

    private Path logPath;
    private Path trackPath;
    private String copyStrategy = DistCpConstants.UNIFORMSIZE;

    private int numListstatusThreads = 0;  // 0 indicates that flag is not set.
    private int maxMaps = DistCpConstants.DEFAULT_MAPS;
    private float mapBandwidth = 0; // 0 indicates we should use the default

    private EnumSet<FileAttribute> preserveStatus =
        EnumSet.noneOf(FileAttribute.class);

    private int blocksPerChunk = 0;

    private int copyBufferSize =
            DistCpConstants.COPY_BUFFER_SIZE_DEFAULT;

    private boolean directWrite = false;

    public Builder(List<Path> sourcePaths, Path targetPath) {
      Preconditions.checkArgument(sourcePaths != null && !sourcePaths.isEmpty(),
          "Source paths should not be null or empty!");
      Preconditions.checkArgument(targetPath != null,
          "Target path should not be null!");
      this.sourcePaths = sourcePaths;
      this.targetPath = targetPath;
    }

    public Builder(Path sourceFileListing, Path targetPath) {
      Preconditions.checkArgument(sourceFileListing != null,
          "Source file listing should not be null!");
      Preconditions.checkArgument(targetPath != null,
          "Target path should not be null!");

      this.sourceFileListing = sourceFileListing;
      this.targetPath = targetPath;
    }

    /**
     * This is the single entry point for constructing DistCpOptions objects.
     *
     * Before a new DistCpOptions object is returned, it will set the dependent
     * options, validate the option combinations. After constructing, the
     * DistCpOptions instance is immutable.
     */
    public DistCpOptions build() {
      setOptionsForSplitLargeFile();

      validate();

      return new DistCpOptions(this);
    }

    /**
     * Override options for split large files.
     */
    private void setOptionsForSplitLargeFile() {
      if (blocksPerChunk <= 0) {
        return;
      }

      LOG.info("Enabling preserving blocksize since "
          + DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch() + " is passed.");
      preserve(FileAttribute.BLOCKSIZE);

      LOG.info("Set " + DistCpOptionSwitch.APPEND.getSwitch()
          + " to false since " + DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch()
          + " is passed.");
      this.append = false;
    }

    private void validate() {
      if ((useDiff || useRdiff) && deleteMissing) {
        // -delete and -diff/-rdiff are mutually exclusive.
        throw new IllegalArgumentException("-delete and -diff/-rdiff are "
            + "mutually exclusive. The -delete option will be ignored.");
      }

      if (!atomicCommit && atomicWorkPath != null) {
        throw new IllegalArgumentException(
            "-tmp work-path can only be specified along with -atomic");
      }

      if (syncFolder && atomicCommit) {
        throw new IllegalArgumentException("Atomic commit can't be used with "
            + "sync folder or overwrite options");
      }

      if (deleteMissing && !(overwrite || syncFolder)) {
        throw new IllegalArgumentException("Delete missing is applicable "
            + "only with update or overwrite options");
      }

      if (overwrite && syncFolder) {
        throw new IllegalArgumentException("Overwrite and update options are "
            + "mutually exclusive");
      }

      if (!syncFolder && append) {
        throw new IllegalArgumentException(
            "Append is valid only with update options");
      }
      if (skipCRC && append) {
        throw new IllegalArgumentException(
            "Append is disallowed when skipping CRC");
      }
      if (!syncFolder && (useDiff || useRdiff)) {
        throw new IllegalArgumentException(
            "-diff/-rdiff is valid only with -update option");
      }

      if (useDiff || useRdiff) {
        if (StringUtils.isBlank(fromSnapshot) ||
            StringUtils.isBlank(toSnapshot)) {
          throw new IllegalArgumentException(
              "Must provide both the starting and ending " +
                  "snapshot names for -diff/-rdiff");
        }
      }
      if (useDiff && useRdiff) {
        throw new IllegalArgumentException(
            "-diff and -rdiff are mutually exclusive");
      }

      if (verboseLog && logPath == null) {
        throw new IllegalArgumentException(
            "-v is valid only with -log option");
      }
    }

    @VisibleForTesting
    Builder withSourcePaths(List<Path> newSourcePaths) {
      this.sourcePaths = newSourcePaths;
      return this;
    }

    public Builder withAtomicCommit(boolean newAtomicCommit) {
      this.atomicCommit = newAtomicCommit;
      return this;
    }

    public Builder withAtomicWorkPath(Path newAtomicWorkPath) {
      this.atomicWorkPath = newAtomicWorkPath;
      return this;
    }

    public Builder withSyncFolder(boolean newSyncFolder) {
      this.syncFolder = newSyncFolder;
      return this;
    }

    public Builder withDeleteMissing(boolean newDeleteMissing) {
      this.deleteMissing = newDeleteMissing;
      return this;
    }

    public Builder withIgnoreFailures(boolean newIgnoreFailures) {
      this.ignoreFailures = newIgnoreFailures;
      return this;
    }

    public Builder withOverwrite(boolean newOverwrite) {
      this.overwrite = newOverwrite;
      return this;
    }

    public Builder withAppend(boolean newAppend) {
      this.append = newAppend;
      return this;
    }

    public Builder withCRC(boolean newSkipCRC) {
      this.skipCRC = newSkipCRC;
      return this;
    }

    public Builder withBlocking(boolean newBlocking) {
      this.blocking = newBlocking;
      return this;
    }

    public Builder withUseDiff(String newFromSnapshot,  String newToSnapshot) {
      this.useDiff = true;
      this.fromSnapshot = newFromSnapshot;
      this.toSnapshot = newToSnapshot;
      return this;
    }

    public Builder withUseRdiff(String newFromSnapshot, String newToSnapshot) {
      this.useRdiff = true;
      this.fromSnapshot = newFromSnapshot;
      this.toSnapshot = newToSnapshot;
      return this;
    }

    public Builder withFiltersFile(String newFiletersFile) {
      this.filtersFile = newFiletersFile;
      return this;
    }

    public Builder withLogPath(Path newLogPath) {
      this.logPath = newLogPath;
      return this;
    }

    public Builder withTrackMissing(Path path) {
      this.trackPath = path;
      return this;
    }

    public Builder withCopyStrategy(String newCopyStrategy) {
      this.copyStrategy = newCopyStrategy;
      return this;
    }

    public Builder withMapBandwidth(float newMapBandwidth) {
      Preconditions.checkArgument(newMapBandwidth > 0,
          "Bandwidth " + newMapBandwidth + " is invalid (should be > 0)");
      this.mapBandwidth = newMapBandwidth;
      return this;
    }

    public Builder withNumListstatusThreads(int newNumListstatusThreads) {
      if (newNumListstatusThreads > MAX_NUM_LISTSTATUS_THREADS) {
        this.numListstatusThreads = MAX_NUM_LISTSTATUS_THREADS;
      } else if (newNumListstatusThreads > 0) {
        this.numListstatusThreads = newNumListstatusThreads;
      } else {
        this.numListstatusThreads = 0;
      }
      return this;
    }

    public Builder maxMaps(int newMaxMaps) {
      this.maxMaps = Math.max(newMaxMaps, 1);
      return this;
    }

    public Builder preserve(String attributes) {
      if (attributes == null || attributes.isEmpty()) {
        preserveStatus = EnumSet.allOf(FileAttribute.class);
      } else {
        for (int index = 0; index < attributes.length(); index++) {
          preserveStatus.add(FileAttribute.
              getAttribute(attributes.charAt(index)));
        }
      }
      return this;
    }

    public Builder preserve(FileAttribute attribute) {
      preserveStatus.add(attribute);
      return this;
    }

    public Builder withBlocksPerChunk(int newBlocksPerChunk) {
      this.blocksPerChunk = newBlocksPerChunk;
      return this;
    }

    public Builder withCopyBufferSize(int newCopyBufferSize) {
      this.copyBufferSize =
          newCopyBufferSize > 0 ? newCopyBufferSize
              : DistCpConstants.COPY_BUFFER_SIZE_DEFAULT;
      return this;
    }

    public Builder withVerboseLog(boolean newVerboseLog) {
      this.verboseLog = newVerboseLog;
      return this;
    }

    public Builder withDirectWrite(boolean newDirectWrite) {
      this.directWrite = newDirectWrite;
      return this;
    }
  }

}
