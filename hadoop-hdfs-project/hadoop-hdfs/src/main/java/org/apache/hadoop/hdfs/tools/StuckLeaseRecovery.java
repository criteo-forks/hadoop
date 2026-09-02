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
package org.apache.hadoop.hdfs.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Finds open files whose last block cannot be recovered by the NameNode on its
 * own and drives {@code recoverLease} against them at a sane cadence.
 * <p>
 * Background. When a writer dies inside pipeline recovery, the NameNode's
 * stored generation stamp for the last block is ahead of every replica, so all
 * replicas are flagged {@code GENSTAMP_MISMATCH}. The block is not COMPLETE, so
 * {@code BlockManager.updateNeededReconstructions} drops it and it never enters
 * {@code QUEUE_WITH_CORRUPT_BLOCKS} - it is invisible to {@code MissingBlocks}
 * and to {@code listCorruptFileBlocks}, and shows up only in the
 * {@code Corrupt Blocks:} section of a metasave.
 * <p>
 * Recovery itself works; the cadence does not.
 * {@code FSNamesystem.internalReleaseLease} renews the lease after every failed
 * attempt, while {@code LeaseManager} only picks leases past the hard limit, so
 * a block needing several rounds gets one round per hard-limit period.
 * {@code recoverLease} with {@code force=true} bypasses the expiry gate and
 * starts a round immediately, which is what this tool does.
 * <p>
 * Note that recovery is asynchronous: the first call returns {@code false}
 * having only queued {@code DNA_RECOVERBLOCK}. The primary datanode picks it up
 * on its next heartbeat and answers with {@code commitBlockSynchronization},
 * which force-completes the block and closes the file - usually within seconds.
 * So this tool polls {@code isFileClosed} on a short interval to notice that,
 * and only re-issues {@code recoverLease} after
 * {@code dfs.heartbeat.interval * 30} has elapsed, since
 * {@code BlockManager.addBlockRecoveryAttempt} rejects anything sooner while
 * {@code internalReleaseLease} still renews the lease and journals.
 */
@InterfaceAudience.Private
public class StuckLeaseRecovery extends Configured implements Tool {

  private static final String TOOL_NAME = "stuckleases";

  /**
   * Holder passed to {@code recoverLease}. {@code force=true} accepts any
   * holder string and releases the lease found from the <em>file's</em> current
   * holder, so this only has to be recognisable on a later pass. Suffixed with
   * the inode id so that files do not share a lease, whose renewal in
   * {@code internalReleaseLease} would otherwise couple them.
   */
  private static final String SWEEPER_HOLDER_PREFIX = "HDFS_StuckLeaseSweeper-";

  private static final int HA_RPC_TIMEOUT_MS = 30000;

  /**
   * Default spacing between {@code recoverLease} calls within one round. Every
   * call takes the FSNamesystem write lock and logSyncs two edit ops - the
   * lease reassignment and the recovery generation stamp - so this is
   * deliberately slack: the files are already stuck, and nothing is gained by
   * issuing a backlog of them as fast as the NameNode will accept them.
   */
  private static final long DEFAULT_ISSUE_DELAY_MS = 500;

  /** Margin added to the suppression window so a re-issue is never rejected. */
  private static final long REISSUE_MARGIN_SEC = 10;

  private static final int DEFAULT_ATTEMPTS = 5;
  private static final int DEFAULT_POLL_SEC = 2;
  private static final int DEFAULT_FSCK_BATCH = 500;

  private static final String GENSTAMP_MISMATCH = "GENSTAMP_MISMATCH";

  /**
   * How a stuck open file was reached, which decides what may be done to it.
   */
  private enum FileClass {
    /** Lease already reassigned to the NameNode's internal holder. */
    NN_STUCK,
    /** Carries this tool's holder, so an earlier pass worked on it. */
    SWEEPER_RETRY,
    /** Lease still held by a client, and the last block is all-corrupt. */
    LIVE_HOLDER_CORRUPT,
    /** Ordinary in-flight writer. */
    OPEN_HEALTHY,
    /** Corrupt but not open: real corruption, not a lease problem. */
    CLOSED_CORRUPT
  }

  /** Whether this host's NameNode may be acted upon. */
  private enum LocalState {
    ACTIVE,
    NOT_ACTIVE,
    UNKNOWN
  }

  private enum Outcome {
    RECOVERED,
    RETRY_NEXT_PASS,
    NEEDS_HUMAN,
    REPORT_ONLY
  }

  /** One replica line from the {@code Corrupt Blocks:} metasave section. */
  private static final class CorruptReplica {
    private final String node;
    private final String reason;

    CorruptReplica(String node, String reason) {
      this.node = node;
      this.reason = reason;
    }
  }

  /** A corrupt block, grouped from its metasave replica lines. */
  private static final class CorruptBlock {
    /** {@code blk_<id>}, the only spelling fsck accepts. */
    private final String blockId;
    /** {@code blk_<id>_<gs>}, as metaSave printed it. */
    private final String blockName;
    private final List<CorruptReplica> replicas = new ArrayList<>();
    private int totalReplicas;

    CorruptBlock(String blockId, String blockName) {
      this.blockId = blockId;
      this.blockName = blockName;
    }

    @Override
    public String toString() {
      Set<String> reasons = new LinkedHashSet<>();
      Set<String> nodes = new LinkedHashSet<>();
      for (CorruptReplica r : replicas) {
        reasons.add(r.reason);
        if (r.node != null) {
          nodes.add(r.node);
        }
      }
      return blockName + " corrupt=" + replicas.size() + "/" + totalReplicas
          + " reason=" + StringUtils.join(",", reasons)
          + " nodes=" + StringUtils.join(",", nodes);
    }
  }

  /** A file the tool has something to say about. */
  private static final class Target {
    private final String path;
    private final String holder;
    private final long inodeId;
    private final FileClass fileClass;
    private final List<CorruptBlock> corruptBlocks;

    private int attempts;
    private Outcome outcome = Outcome.REPORT_ONLY;
    private String detail = "";

    Target(String path, String holder, long inodeId, FileClass fileClass,
        List<CorruptBlock> corruptBlocks) {
      this.path = path;
      this.holder = holder;
      this.inodeId = inodeId;
      this.fileClass = fileClass;
      this.corruptBlocks = corruptBlocks == null
          ? new ArrayList<CorruptBlock>() : corruptBlocks;
    }
  }

  /** The local NameNode this pass is bound to. */
  private static final class LocalNameNode {
    private final String nsId;
    private final String nnId;

    LocalNameNode(String nsId, String nnId) {
      this.nsId = nsId;
      this.nnId = nnId;
    }

    @Override
    public String toString() {
      return nsId == null ? "(single namespace)"
          : (nnId == null ? nsId : nsId + "." + nnId);
    }
  }

  /** Parsed command line. */
  private static final class Options {
    private String nameservice;
    private String path = "/";
    private boolean execute;
    private boolean includeLiveHolders;
    private boolean full;
    private String logDir;
    private int attempts = DEFAULT_ATTEMPTS;
    private long pollMs = TimeUnit.SECONDS.toMillis(DEFAULT_POLL_SEC);
    private long issueDelayMs = DEFAULT_ISSUE_DELAY_MS;
    private long reissueMs = -1;
    private int fsckBatch = DEFAULT_FSCK_BATCH;
    private int maxFiles = Integer.MAX_VALUE;
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: hdfs " + TOOL_NAME + " [options]");
    out.println();
    out.println("Report, and optionally recover, open files whose last block");
    out.println("cannot be recovered by the NameNode on its own. Runs against");
    out.println("the active NameNode only; exits 0 on a standby.");
    out.println();
    out.println("  -ns <nameservice>     the local nameservice to work on;"
        + " defaults to the one");
    out.println("                        whose NameNode runs on this host");
    out.println("  -path <prefix>        limit to this path prefix"
        + " (default /)");
    out.println("  -execute              actually call recoverLease"
        + " (default: report only)");
    out.println("  -includeLiveHolders   also recover files whose lease is"
        + " still held by a client");
    out.println("  -full                 run the metasave + fsck pass, needed"
        + " to see corruption");
    out.println("  -logDir <dir>         NameNode log dir holding the metasave"
        + " output");
    out.println("  -attempts <n>         recoverLease attempts per file per"
        + " pass (default " + DEFAULT_ATTEMPTS + ")");
    out.println("  -pollSec <n>          isFileClosed poll interval (default "
        + DEFAULT_POLL_SEC + ")");
    out.println("  -reissueSec <n>       gap before a new recoverLease"
        + " (default: the NameNode's block");
    out.println("                        recovery timeout + "
        + REISSUE_MARGIN_SEC + "s)");
    out.println("  -issueDelayMs <n>     delay between recoverLease calls"
        + " (default " + DEFAULT_ISSUE_DELAY_MS + ")");
    out.println("  -fsckBatch <n>        block ids per fsck call (default "
        + DEFAULT_FSCK_BATCH + ")");
    out.println("  -maxFiles <n>         cap on files acted upon");
    out.println();
    ToolRunner.printGenericCommandUsage(out);
  }

  @Override
  public int run(String[] argv) throws Exception {
    List<String> args = new LinkedList<>(Arrays.asList(argv));
    if (StringUtils.popOption("-help", args)
        || StringUtils.popOption("-h", args)) {
      printUsage(System.out);
      return 0;
    }

    Options opts;
    try {
      opts = parseOptions(args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      printUsage(System.err);
      return 1;
    }

    LocalNameNode local = resolveLocalNameNode(getConf(), opts.nameservice);
    if (local == null) {
      return 1;
    }

    // Under federation fs.defaultFS is a mount table, or some other
    // nameservice entirely, so bind every RPC to the local NameNode's own
    // address instead. initializeGenericKeys copies the suffixed
    // dfs.namenode.* keys for this nsId/nnId into the unsuffixed ones and
    // rewrites fs.defaultFS to match, which is how the NameNode itself and
    // NNHAServiceTarget resolve their target.
    Configuration conf = new HdfsConfiguration(getConf());
    NameNode.initializeGenericKeys(conf, local.nsId, local.nnId);
    URI nnUri = FileSystem.getDefaultUri(conf);

    LocalState state = localNameNodeState(conf, local);
    if (state == LocalState.UNKNOWN) {
      return 1;
    }
    if (state == LocalState.NOT_ACTIVE) {
      System.out.println("Local NameNode is not ACTIVE; nothing to do.");
      return 0;
    }

    // Addressing the NameNode directly rather than through a logical HA URI is
    // deliberate: metaSave writes into the log dir of whichever NameNode serves
    // it, and this pass reads that file locally. A failover mid-pass then
    // surfaces as a StandbyException and stops us, instead of silently
    // continuing against a host whose log dir we cannot read.
    try (DistributedFileSystem dfs = AdminHelper.getDFS(nnUri, conf)) {
      System.out.println("Working on " + local + " via " + nnUri + ".");
      // path -> corrupt blocks, empty unless -full was given.
      Map<String, List<CorruptBlock>> corruptByPath = opts.full
          ? findCorruptPaths(dfs, conf, nnUri, opts)
          : new LinkedHashMap<String, List<CorruptBlock>>();

      List<Target> targets = classify(dfs, opts, corruptByPath);

      if (opts.execute) {
        recover(dfs, opts, actionable(targets, opts));
      }
      return report(targets, opts);
    }
  }

  private Options parseOptions(List<String> args) {
    Options opts = new Options();
    opts.nameservice = StringUtils.popOptionWithArgument("-ns", args);
    String value = StringUtils.popOptionWithArgument("-path", args);
    if (value != null) {
      opts.path = value;
    }
    opts.execute = StringUtils.popOption("-execute", args);
    opts.includeLiveHolders =
        StringUtils.popOption("-includeLiveHolders", args);
    opts.full = StringUtils.popOption("-full", args);
    opts.logDir = StringUtils.popOptionWithArgument("-logDir", args);
    opts.attempts = (int) popPositive("-attempts", args, opts.attempts);
    opts.pollMs = TimeUnit.SECONDS.toMillis(
        popPositive("-pollSec", args, DEFAULT_POLL_SEC));
    value = StringUtils.popOptionWithArgument("-reissueSec", args);
    if (value != null) {
      opts.reissueMs = TimeUnit.SECONDS.toMillis(parsePositive("-reissueSec",
          value));
    }
    opts.issueDelayMs =
        popPositive("-issueDelayMs", args, opts.issueDelayMs);
    opts.fsckBatch = (int) popPositive("-fsckBatch", args, opts.fsckBatch);
    opts.maxFiles = (int) popPositive("-maxFiles", args, opts.maxFiles);

    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unrecognised argument: "
          + args.get(0));
    }
    if (opts.reissueMs < 0) {
      long heartbeatSec = getConf().getLong(
          DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
          DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
      opts.reissueMs = BlockManager.getBlockRecoveryTimeout(heartbeatSec)
          + TimeUnit.SECONDS.toMillis(REISSUE_MARGIN_SEC);
    }
    if (opts.includeLiveHolders && !opts.full) {
      throw new IllegalArgumentException("-includeLiveHolders needs -full, "
          + "since LIVE_HOLDER_CORRUPT can only be identified from a "
          + "metasave.");
    }
    return opts;
  }

  private static long popPositive(String name, List<String> args,
      long defaultValue) {
    String value = StringUtils.popOptionWithArgument(name, args);
    return value == null ? defaultValue : parsePositive(name, value);
  }

  private static long parsePositive(String name, String value) {
    long parsed;
    try {
      parsed = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse the argument to "
          + name + ": " + value);
    }
    if (parsed <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got "
          + parsed);
    }
    return parsed;
  }

  /**
   * Identifies the nameservice, and within it the NameNode, that this host
   * serves. Both lookups work by matching configured RPC addresses against
   * this host's addresses, which is what makes them federation-aware; a host
   * serving more than one nameservice needs {@code -ns} to disambiguate.
   *
   * @return null when the local NameNode could not be identified
   */
  private LocalNameNode resolveLocalNameNode(Configuration conf,
      String nameservice) {
    try {
      String nsId = nameservice != null
          ? nameservice : DFSUtil.getNamenodeNameServiceId(conf);
      String nnId = HAUtil.isHAEnabled(conf, nsId)
          ? HAUtil.getNameNodeId(conf, nsId) : null;
      return new LocalNameNode(nsId, nnId);
    } catch (RuntimeException e) {
      if (DFSUtilClient.getNameServiceIds(conf).isEmpty()) {
        // No nameservice configured at all: a plain single NameNode, where
        // fs.defaultFS already points at the only one there is.
        return new LocalNameNode(null, null);
      }
      // Fail closed rather than risk working against the wrong namespace.
      System.err.println("Cannot identify which NameNode on this host to work "
          + "on; pass -ns <nameservice>. " + AdminHelper.prettifyException(e));
      return null;
    }
  }

  /**
   * ACTIVE when this host's NameNode is the active one, or when HA is not
   * configured. Lets the same cron entry be installed on every NameNode.
   * UNKNOWN when the state could not be established, which must not be
   * mistaken for a standby.
   */
  private LocalState localNameNodeState(Configuration conf,
      LocalNameNode local) {
    if (local.nnId == null) {
      // Not an HA nameservice: there is no standby to be confused with.
      return LocalState.ACTIVE;
    }

    HAServiceProtocol proxy = null;
    try {
      proxy = new NNHAServiceTarget(conf, local.nsId, local.nnId)
          .getProxy(conf, HA_RPC_TIMEOUT_MS);
      HAServiceState state = proxy.getServiceStatus().getState();
      System.out.println("Local NameNode " + local + " is " + state + ".");
      return state == HAServiceState.ACTIVE
          ? LocalState.ACTIVE : LocalState.NOT_ACTIVE;
    } catch (IOException e) {
      System.err.println("Failed to query the HA state of " + local + ": "
          + AdminHelper.prettifyException(e));
      return LocalState.UNKNOWN;
    } finally {
      if (proxy != null) {
        try {
          RPC.stopProxy(proxy);
        } catch (RuntimeException e) {
          // Not a stoppable proxy; nothing to release.
        }
      }
    }
  }

  /**
   * Runs the metasave + fsck pass. These blocks are not COMPLETE, so they are
   * absent from every other enumeration the NameNode offers; the
   * {@code Corrupt Blocks:} metasave section is the only source.
   */
  private Map<String, List<CorruptBlock>> findCorruptPaths(
      DistributedFileSystem dfs, Configuration conf, URI nnUri, Options opts)
      throws IOException {
    File logDir = resolveLogDir(opts);
    String fileName = TOOL_NAME + "-" + System.currentTimeMillis()
        + ".metasave";
    File metaSaveFile = new File(logDir, fileName);

    System.out.println("Requesting a metasave into " + metaSaveFile
        + " (this takes the FSNamesystem read lock).");
    dfs.getClient().getNamenode().metaSave(fileName);

    Map<String, CorruptBlock> blocks;
    try {
      blocks = parseCorruptBlocks(metaSaveFile);
    } finally {
      if (metaSaveFile.exists() && !metaSaveFile.delete()) {
        System.err.println("Could not delete " + metaSaveFile
            + "; remove it by hand.");
      }
    }

    if (blocks.isEmpty()) {
      System.out.println("No " + GENSTAMP_MISMATCH
          + " blocks in the metasave.");
      return new LinkedHashMap<>();
    }
    System.out.println("Found " + blocks.size() + " block(s) with all "
        + "replicas reporting " + GENSTAMP_MISMATCH + ".");

    Map<String, String> blockToPath =
        resolvePaths(conf, nnUri, blocks.values(), opts.fsckBatch);

    Map<String, List<CorruptBlock>> byPath = new LinkedHashMap<>();
    for (CorruptBlock block : blocks.values()) {
      String path = blockToPath.get(block.blockId);
      if (path == null) {
        System.err.println("fsck could not map " + block.blockId
            + " to a path; skipping it.");
        continue;
      }
      if (!isUnderPrefix(path, opts.path)) {
        continue;
      }
      List<CorruptBlock> list = byPath.get(path);
      if (list == null) {
        list = new ArrayList<>();
        byPath.put(path, list);
      }
      list.add(block);
    }
    return byPath;
  }

  private File resolveLogDir(Options opts) throws IOException {
    String dir = opts.logDir;
    if (dir == null) {
      dir = System.getenv("HADOOP_LOG_DIR");
    }
    if (dir == null) {
      dir = System.getProperty("hadoop.log.dir");
    }
    if (dir == null) {
      throw new IOException("Cannot locate the NameNode log directory. Pass "
          + "-logDir; the NameNode writes the metasave to its own "
          + "hadoop.log.dir.");
    }
    File file = new File(dir);
    if (!file.isDirectory()) {
      throw new IOException("Not a directory: " + file);
    }
    return file;
  }

  /**
   * Parses the {@code Corrupt Blocks:} section, whose lines are tab separated
   * {@code key=value} pairs starting with {@code Block=}. Only blocks whose
   * every listed replica is {@code GENSTAMP_MISMATCH} are kept - those are the
   * ones lease recovery can act on. The generation stamp printed in the block
   * name is the first <em>reported</em> one, not the NameNode's stored value,
   * so it is carried through only as a label.
   */
  private Map<String, CorruptBlock> parseCorruptBlocks(File metaSaveFile)
      throws IOException {
    Map<String, CorruptBlock> blocks = new LinkedHashMap<>();
    int otherReason = 0;

    for (String line : Files.readAllLines(metaSaveFile.toPath(),
        StandardCharsets.UTF_8)) {
      if (!line.startsWith("Block=")) {
        continue;
      }
      Map<String, String> fields = new LinkedHashMap<>();
      for (String field : line.split("\t")) {
        int eq = field.indexOf('=');
        if (eq > 0) {
          fields.put(field.substring(0, eq), field.substring(eq + 1));
        }
      }
      String blockName = fields.get("Block");
      String reason = fields.get("Reason");
      if (blockName == null || reason == null
          || !blockName.startsWith(Block.BLOCK_FILE_PREFIX)) {
        continue;
      }
      if (!GENSTAMP_MISMATCH.equals(reason)) {
        otherReason++;
        continue;
      }
      // Key by blk_<id>: corruptReplicasMap is keyed by block id alone, so
      // there is never more than one entry per id to collapse, and blk_<id> is
      // what the fsck leg has to be handed.
      String blockId = stripGenerationStamp(blockName);
      CorruptBlock block = blocks.get(blockId);
      if (block == null) {
        block = new CorruptBlock(blockId, blockName);
        blocks.put(blockId, block);
      }
      block.replicas.add(new CorruptReplica(fields.get("Node"), reason));
      block.totalReplicas =
          parseIntOr(fields.get("TotalReplicas"), block.totalReplicas);
    }

    if (otherReason > 0) {
      System.out.println("Ignored " + otherReason + " corrupt replica line(s) "
          + "with a reason other than " + GENSTAMP_MISMATCH + ".");
    }

    // Only an all-replicas-corrupt block is stuck; one with a good replica
    // left is ordinary corruption the BlockManager will handle.
    Iterator<Map.Entry<String, CorruptBlock>> it =
        blocks.entrySet().iterator();
    while (it.hasNext()) {
      CorruptBlock block = it.next().getValue();
      if (block.totalReplicas > block.replicas.size()) {
        it.remove();
      }
    }
    return blocks;
  }

  /**
   * Turns the {@code blk_<id>_<gs>} that metaSave prints into the
   * {@code blk_<id>} that fsck accepts. {@code Block.getBlockId} matches only
   * {@code blk_<id>} or {@code blk_<id>_<gs>.meta}; handed the bare
   * {@code blk_<id>_<gs>} it silently returns 0, and fsck then reports that
   * block 0 does not exist.
   */
  private static String stripGenerationStamp(String blockName) {
    int gs = blockName.indexOf('_', Block.BLOCK_FILE_PREFIX.length());
    return gs < 0 ? blockName : blockName.substring(0, gs);
  }

  private static int parseIntOr(String value, int fallback) {
    if (value == null) {
      return fallback;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  /**
   * Maps block names to paths by driving {@link DFSck} in process, which
   * already handles SPNEGO and HA-aware URL selection.
   * <p>
   * The ids travel in a GET query string, so the batch size is bounded by the
   * NameNode's Jetty {@code hadoop.http.max.request.header.size} (64 KB by
   * default), which covers the request line and every header together;
   * overshooting yields a 414 that surfaces as an opaque IOException.
   * The FSNamesystem read lock is taken and released per block id, so a large
   * batch does not hold it for the whole batch.
   */
  private Map<String, String> resolvePaths(Configuration conf, URI nnUri,
      Collection<CorruptBlock> blocks, int batchSize) throws IOException {
    Map<String, String> blockToPath = new LinkedHashMap<>();
    List<String> batch = new ArrayList<>(batchSize);

    for (CorruptBlock block : blocks) {
      batch.add(block.blockId);
      if (batch.size() == batchSize) {
        blockToPath.putAll(runFsck(conf, nnUri, batch));
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      blockToPath.putAll(runFsck(conf, nnUri, batch));
    }
    return blockToPath;
  }

  private Map<String, String> runFsck(Configuration conf, URI nnUri,
      List<String> batch) throws IOException {
    List<String> args = new ArrayList<>(batch.size() + 2);
    // DFSck picks its fsck URL from the filesystem of its path argument, so
    // name the local NameNode explicitly - the default would be fs.defaultFS,
    // which under federation is the wrong namespace. The path has to come
    // first: -blockId swallows every following argument that does not start
    // with a dash.
    args.add(nnUri.toString() + "/");
    args.add("-blockId");
    args.addAll(batch);

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    // DFSck runs under the ambient Kerberos context and drives SPNEGO from it.
    try (PrintStream capture =
        new PrintStream(buffer, true, StandardCharsets.UTF_8.name())) {
      // The return value is unusable on the -blockId path: NamenodeFsck.fsck
      // returns before printing any HEALTHY/CORRUPT line, so DFSck's lastLine
      // ladder can leave errCode at -1 on a perfectly good run.
      new DFSck(conf, capture).run(args.toArray(new String[0]));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("fsck failed for " + batch.size()
          + " block id(s)", e);
    }

    Map<String, String> blockToPath = new LinkedHashMap<>();
    String blockId = null;
    for (String line : buffer.toString(StandardCharsets.UTF_8.name())
        .split("\n")) {
      line = line.trim();
      if (line.startsWith("Block Id: ")) {
        // blockIdCK echoes back exactly the string it was handed.
        blockId = line.substring("Block Id: ".length()).trim();
      } else if (line.startsWith("Block belongs to: ")) {
        String path = line.substring("Block belongs to: ".length()).trim();
        if (blockId != null) {
          blockToPath.put(blockId, path);
          blockId = null;
        }
      }
    }
    return blockToPath;
  }

  /**
   * Joins the open-files listing with the corrupt set. The holder decides what
   * is safe: reaching {@code reassignLease} means the original client stopped
   * renewing for a whole hard-limit period <em>and</em> the NameNode started
   * recovery, so that writer is finished either way.
   */
  private List<Target> classify(DistributedFileSystem dfs, Options opts,
      Map<String, List<CorruptBlock>> corruptByPath) throws IOException {
    List<Target> targets = new ArrayList<>();
    Set<String> openPaths = new LinkedHashSet<>();
    int openHealthy = 0;

    RemoteIterator<OpenFileEntry> it = dfs.listOpenFiles(
        EnumSet.of(OpenFilesType.ALL_OPEN_FILES), opts.path);
    while (it.hasNext()) {
      OpenFileEntry entry = it.next();
      String path = entry.getFilePath();
      String holder = entry.getClientName();
      openPaths.add(path);

      List<CorruptBlock> corrupt = corruptByPath.get(path);
      FileClass fileClass;
      if (holder != null
          && holder.startsWith(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) {
        fileClass = FileClass.NN_STUCK;
      } else if (holder != null
          && holder.startsWith(SWEEPER_HOLDER_PREFIX)) {
        fileClass = FileClass.SWEEPER_RETRY;
      } else if (corrupt != null) {
        fileClass = FileClass.LIVE_HOLDER_CORRUPT;
      } else {
        openHealthy++;
        continue;
      }
      targets.add(new Target(path, holder, entry.getId(), fileClass, corrupt));
    }

    for (Map.Entry<String, List<CorruptBlock>> entry
        : corruptByPath.entrySet()) {
      if (!openPaths.contains(entry.getKey())) {
        targets.add(new Target(entry.getKey(), null, -1,
            FileClass.CLOSED_CORRUPT, entry.getValue()));
      }
    }

    System.out.println("Skipped " + openHealthy + " "
        + FileClass.OPEN_HEALTHY + " file(s) with no sign of a problem.");
    return targets;
  }

  private List<Target> actionable(List<Target> targets, Options opts) {
    List<Target> work = new ArrayList<>();
    for (Target target : targets) {
      boolean act = target.fileClass == FileClass.NN_STUCK
          || target.fileClass == FileClass.SWEEPER_RETRY
          || (target.fileClass == FileClass.LIVE_HOLDER_CORRUPT
              && opts.includeLiveHolders);
      if (act && work.size() < opts.maxFiles) {
        work.add(target);
      }
    }
    return work;
  }

  /**
   * Drives recovery in rounds across the whole working set, so a pass costs at
   * most {@code attempts * reissueSec} in wall clock however many files it is
   * working on, rather than that per file.
   */
  private void recover(DistributedFileSystem dfs, Options opts,
      List<Target> work) throws IOException {
    if (work.isEmpty()) {
      System.out.println("Nothing actionable to recover.");
      return;
    }
    // Each round issues one recoverLease per file, then polls for the whole
    // suppression window, so say up front roughly how long this will take.
    long roundSec = TimeUnit.MILLISECONDS.toSeconds(
        work.size() * opts.issueDelayMs + opts.reissueMs);
    System.out.println("Recovering " + work.size() + " file(s), up to "
        + opts.attempts + " attempt(s) each, re-issuing every "
        + TimeUnit.MILLISECONDS.toSeconds(opts.reissueMs) + "s; at most "
        + (opts.attempts * roundSec) + "s if none of them close early.");

    ClientProtocol namenode = dfs.getClient().getNamenode();
    List<Target> pending = new LinkedList<>(work);
    try {
      runRounds(dfs, opts, namenode, pending);
    } finally {
      for (Target target : pending) {
        target.outcome = Outcome.RETRY_NEXT_PASS;
        target.detail = "still open after " + target.attempts + " attempt(s)";
      }
    }
  }

  private void runRounds(DistributedFileSystem dfs, Options opts,
      ClientProtocol namenode, List<Target> pending) throws IOException {
    for (int round = 1; round <= opts.attempts && !pending.isEmpty();
        round++) {
      Iterator<Target> it = pending.iterator();
      while (it.hasNext()) {
        Target target = it.next();
        target.attempts++;
        try {
          if (namenode.recoverLease(target.path,
              SWEEPER_HOLDER_PREFIX + target.inodeId)) {
            target.outcome = Outcome.RECOVERED;
            target.detail = "closed on attempt " + target.attempts;
            it.remove();
          }
        } catch (IOException e) {
          if (!handleRecoverLeaseFailure(target, e)) {
            // Cluster-wide condition: leave the rest for the next pass.
            return;
          }
          it.remove();
        }
        if (!sleepFor(opts.issueDelayMs)) {
          return;
        }
      }

      // The common case closes seconds after the first call, so watch for it
      // with isFileClosed - read-only, where recoverLease takes the write lock
      // and journals - and only re-issue once the suppression window is past.
      long deadline = Time.monotonicNow() + opts.reissueMs;
      while (!pending.isEmpty() && Time.monotonicNow() < deadline) {
        if (!sleepFor(Math.min(opts.pollMs, deadline - Time.monotonicNow()))) {
          return;
        }
        Iterator<Target> poll = pending.iterator();
        while (poll.hasNext()) {
          Target target = poll.next();
          try {
            if (dfs.isFileClosed(new Path(target.path))) {
              target.outcome = Outcome.RECOVERED;
              target.detail = "closed after attempt " + target.attempts;
              poll.remove();
            }
          } catch (FileNotFoundException e) {
            target.outcome = Outcome.NEEDS_HUMAN;
            target.detail = "path disappeared during recovery";
            poll.remove();
          }
        }
      }
    }
  }

  /**
   * Classifies a {@code recoverLease} failure. Returns true when the file can
   * be dropped from the working set and the pass may continue, false when the
   * condition is cluster wide and the pass should stop.
   */
  private boolean handleRecoverLeaseFailure(Target target, IOException raw) {
    IOException e = raw instanceof RemoteException
        ? ((RemoteException) raw).unwrapRemoteException() : raw;

    if (e instanceof SafeModeException || e instanceof RetriableException) {
      // checkNameNodeSafeMode throws one or the other depending on whether the
      // caller is expected to retry; either way no write will be accepted now.
      System.err.println("NameNode is not accepting writes, stopping this "
          + "pass: " + e.getMessage());
      return false;
    }
    if (e instanceof AccessControlException) {
      System.err.println("Not permitted to recover leases: "
          + e.getMessage());
      return false;
    }
    if (e instanceof FileNotFoundException) {
      target.outcome = Outcome.NEEDS_HUMAN;
      target.detail = "path no longer exists";
      return true;
    }
    if (e instanceof AlreadyBeingCreatedException) {
      target.outcome = Outcome.NEEDS_HUMAN;
      // internalReleaseLease throws this before attempting any recovery when
      // the penultimate block is below min storage, so retrying cannot help.
      target.detail = e.getMessage() != null
          && e.getMessage().contains("waiting to be minimally replicated")
          ? "penultimate block below min storage; recoverLease can never "
              + "close this file"
          : "recoverLease rejected: " + e.getMessage();
      return true;
    }
    target.outcome = Outcome.NEEDS_HUMAN;
    target.detail = "recoverLease failed: " + AdminHelper.prettifyException(e);
    return true;
  }

  /** Returns false if interrupted, so the caller can wind the pass down. */
  private boolean sleepFor(long millis) {
    if (millis <= 0) {
      return true;
    }
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Interrupted; stopping this pass.");
      return false;
    }
  }

  /**
   * Prints one line per interesting file. Exits non-zero only on NEEDS_HUMAN;
   * RETRY_NEXT_PASS is the normal cross-pass continuation, not a failure.
   */
  private int report(List<Target> targets, Options opts) {
    if (targets.isEmpty()) {
      System.out.println("No stuck open files found.");
      return 0;
    }

    int needsHuman = 0;
    System.out.println();
    for (Target target : targets) {
      if (!opts.execute) {
        target.outcome = Outcome.REPORT_ONLY;
      } else if (target.fileClass == FileClass.LIVE_HOLDER_CORRUPT
          && !opts.includeLiveHolders) {
        target.detail = "lease still held by a client; pass "
            + "-includeLiveHolders to act on it";
      }
      if (target.outcome == Outcome.NEEDS_HUMAN) {
        needsHuman++;
      }

      StringBuilder line = new StringBuilder();
      line.append(target.fileClass).append('\t')
          .append(target.outcome).append('\t')
          .append(target.path)
          .append("\tholder=").append(target.holder)
          .append("\tinode=").append(target.inodeId)
          .append("\tattempts=").append(target.attempts);
      for (CorruptBlock block : target.corruptBlocks) {
        line.append("\t").append(block);
      }
      if (!target.detail.isEmpty()) {
        line.append("\t(").append(target.detail).append(')');
      }
      System.out.println(line);
    }

    System.out.println();
    if (!opts.execute) {
      System.out.println("Report only; pass -execute to call recoverLease.");
    }
    if (!opts.full) {
      System.out.println("Ran without -full, so corruption was not checked; "
          + "LIVE_HOLDER_CORRUPT and CLOSED_CORRUPT cannot be seen.");
    }
    return needsHuman > 0 ? 1 : 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HdfsConfiguration(),
        new StuckLeaseRecovery(), args);
    System.exit(res);
  }

  private static boolean isUnderPrefix(String path, String prefix) {
    if (prefix == null || prefix.isEmpty() || "/".equals(prefix)) {
      return true;
    }
    String normalised = prefix.endsWith("/") ? prefix : prefix + "/";
    return path.equals(prefix) || path.startsWith(normalised);
  }
}
