/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_KILL_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_PRESSURE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;

/**
 * The out of memory handler for cgroup v2. It is a {@link DefaultOOMHandler}
 * with what the cgroup version decides overridden: the out of memory
 * condition, the memory measure, how a container is killed and the pause
 * after a kill. The victim policy of {@link DefaultOOMHandler#run()} is
 * untouched, and so is every v1 path.
 *
 * cgroup v2 has no oom_kill_disable, so the kernel cannot be asked to freeze
 * the cgroup at its limit: memory.max is always a kill. The node manager
 * therefore gets its warning from memory.high, which
 * {@link CGroupsV2ElasticMemoryController} sets below memory.max and which
 * only throttles and reclaims, and this handler kills a container of its
 * choosing before memory.max is reached.
 *
 * A memory.high event is only a wake-up. The kernel raises it before it even
 * attempts reclaim, on a total usage that includes the page cache, so it says
 * nothing about whether the footprint is reclaimable. The condition that
 * actually justifies a kill is the memory footprint of
 * {@link CGroupsV2MemoryStat} standing above memory.high without
 * interruption for the configured hold duration, optionally narrowed by the
 * kernel reporting that memory really did stall.
 *
 * That verdict takes time to settle, so {@link #isUnderOOM()} blocks until
 * it has: the kill loop kills when the footprint held, and returns when it
 * dipped, which is what happens when a container frees its memory or exits
 * by itself.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CGroupsV2OOMHandler extends DefaultOOMHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CGroupsV2OOMHandler.class);

  /**
   * How long to wait for cgroup.kill to empty a container cgroup before
   * carrying on. The kernel delivers the signals synchronously, so this only
   * covers the reaping of the processes.
   */
  private static final int CGROUP_KILL_WAIT_ATTEMPTS = 100;
  private static final int CGROUP_KILL_WAIT_INTERVAL_MS = 10;

  /** How often {@link #awaitOOM()} re-evaluates a pending verdict. */
  private static final long OOM_POLL_INTERVAL_MS = 100;

  /**
   * How often {@link #awaitOOM()} repeats that the footprint is held over
   * memory.high past the hold duration and only the kernel pressure term
   * keeps it from killing. A node can sit there for a long time, legitimately.
   */
  private static final long PENDING_LOG_INTERVAL_MS = 60000;

  /**
   * The out of memory verdict. PENDING is over memory.high but not yet
   * for the hold duration, or held but without the kernel reporting a stall.
   */
  @VisibleForTesting
  enum Verdict { CLEAR, PENDING, KILL }

  private final Clock clock = new MonotonicClock();
  private final boolean enforceVirtualMemory;
  private final String yarnCGroupPath;
  private final long holdDurationMs;
  private final long postKillDelayMs;
  private final boolean pressureEnabled;
  private final ElasticMemoryMetrics metrics;

  /**
   * When the footprint condition started holding, 0 when it does not hold.
   * Guarded by the monitor of this object.
   */
  private long firstBreachAtMs;

  /** The previous sample of memory.pressure's full total, -1 for none. */
  private long lastPressureTotalUs = -1;

  /**
   * Create an OOM handler on the configuration and the cgroups handler of
   * the node manager. This has to be public to be able to construct through
   * reflection.
   * @param context node manager context to work with
   * @param enforceVirtualMemory true if virtual memory needs to be checked,
   *                   false if physical memory needs to be checked instead
   */
  public CGroupsV2OOMHandler(Context context, boolean enforceVirtualMemory) {
    this(context, enforceVirtualMemory, context.getConf(),
        ResourceHandlerModule.getCGroupsHandler(), ElasticMemoryMetrics.create());
  }

  /**
   * Create an OOM handler.
   * @param context node manager context to work with
   * @param enforceVirtualMemory true if virtual memory needs to be checked,
   *                   false if physical memory needs to be checked instead
   * @param conf Yarn configuration to read the hold duration, the pressure
   *             switch and the post kill delay from
   * @param cgroups the cgroups handler to read and write cgroups with
   */
  public CGroupsV2OOMHandler(Context context, boolean enforceVirtualMemory,
      Configuration conf, CGroupsHandler cgroups) {
    this(context, enforceVirtualMemory, conf, cgroups,
        ElasticMemoryMetrics.create());
  }

  /**
   * Create an OOM handler publishing its decisions to the supplied source.
   * @param context node manager context to work with
   * @param enforceVirtualMemory whether swap counts towards the footprint
   * @param conf Yarn configuration
   * @param cgroups cgroups handler
   * @param metrics elastic memory metrics source
   */
  CGroupsV2OOMHandler(Context context, boolean enforceVirtualMemory,
      Configuration conf, CGroupsHandler cgroups,
      ElasticMemoryMetrics metrics) {
    super(context, enforceVirtualMemory, cgroups);
    this.enforceVirtualMemory = enforceVirtualMemory;
    this.yarnCGroupPath = cgroups.getPathForCGroup(
        CGroupsHandler.CGroupController.MEMORY, "");
    this.holdDurationMs = conf.getLong(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS);
    this.postKillDelayMs = conf.getLong(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS);
    this.pressureEnabled = conf.getBoolean(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED)
        && isPressureReadable();
    this.metrics = metrics;
  }

  /**
   * The out-of-memory condition: the memory footprint of the root YARN cgroup
   * has been above memory.high continuously for the configured hold duration.
   *
   * This is not memory.current above memory.high. memory.current counts the
   * page cache, which the kernel reclaims on its own, so it would have us
   * kill containers over a cache that was about to be dropped anyway.
   *
   * The hold duration is oomd's PressureAbove hysteresis applied to the
   * footprint: the timer resets on any dip, so a condition that flaps never
   * kills. It works the same way with kernel pressure information switched
   * off, which is the normal state of our kernels.
   *
   * A memory.high event wakes the kill loop up before the verdict can be
   * known, so this blocks while the verdict is pending. See
   * {@link #awaitOOM()}.
   */
  @Override
  protected boolean isUnderOOM() throws ResourceHandlerException {
    return awaitOOM();
  }

  /**
   * One evaluation of the condition. Distinguishes a footprint that is not
   * over memory.high, where nothing is pending, from one that is but has not
   * held long enough yet, which is what {@link #awaitOOM()} waits on.
   */
  @VisibleForTesting
  synchronized Verdict evaluate() throws ResourceHandlerException {
    long now = clock.getTime();
    long footprint = readFootprint();
    boolean over = footprint > readHighWatermark();
    metrics.memoryFootprintBytes.set(footprint);
    if (!over) {
      firstBreachAtMs = 0;
    } else if (firstBreachAtMs == 0) {
      firstBreachAtMs = now;
    }
    boolean held = firstBreachAtMs != 0
        && now - firstBreachAtMs >= holdDurationMs;
    // Sampled on every call, whether it is needed or not, so that the delta is
    // always between two consecutive polls.
    boolean stalling = sampleMemoryStall();
    Verdict verdict;
    if (!over) {
      verdict = Verdict.CLEAR;
    } else {
    // The pressure term can only ever narrow the condition. It must never be
    // able to trigger a kill the footprint condition alone would not.
      verdict = held && stalling ? Verdict.KILL : Verdict.PENDING;
    }
    metrics.setVerdict(verdict);
    metrics.breachDurationMs.set(firstBreachAtMs == 0
        ? 0 : now - firstBreachAtMs);
    return verdict;
  }

  /**
   * Block while the verdict is pending, re-evaluating every
   * {@link #OOM_POLL_INTERVAL_MS}. The hold itself is expected and silent. A
   * verdict still pending past the hold can only be the kernel pressure term
   * holding it back, which is logged once and then every
   * {@link #PENDING_LOG_INTERVAL_MS}.
   *
   * @return true when the footprint held above memory.high for the hold
   *         duration, so a container has to be killed; false when it dipped,
   *         so there is nothing to do this time
   * @throws ResourceHandlerException a cgroup file could not be read
   */
  @VisibleForTesting
  boolean awaitOOM() throws ResourceHandlerException {
    long lastLog = -1;
    while (true) {
      Verdict verdict = evaluate();
      if (verdict != Verdict.PENDING) {
        return verdict == Verdict.KILL;
      }
      long now = clock.getTime();
      long overFor = now - breachStartMs();
      if (overFor >= holdDurationMs
          && (lastLog < 0 || now - lastLog >= PENDING_LOG_INTERVAL_MS)) {
        LOG.warn("{} has had its memory footprint over memory.high for {} ms,"
            + " past the {} ms hold, but the kernel reports no memory stall."
            + " Waiting for one before killing a container", yarnCGroupPath,
            overFor, holdDurationMs);
        lastLog = now;
      }
      try {
        Thread.sleep(OOM_POLL_INTERVAL_MS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  private synchronized long breachStartMs() {
    return firstBreachAtMs;
  }

  private long readFootprint() throws ResourceHandlerException {
    return CGroupsV2MemoryStat.readFootprint(cgroups, "",
        enforceVirtualMemory);
  }

  private long readHighWatermark() throws ResourceHandlerException {
    long high = CGroupsV2MemoryStat.parseLimit(cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, "", CGROUP_MEMORY_HIGH));
    return high;
  }

  /**
   * Whether the kernel reports that memory stalled since the previous sample.
   * Returns true, that is it does not narrow anything, when pressure
   * information is unavailable or switched off.
   */
  private boolean sampleMemoryStall() {
    if (!pressureEnabled) {
      return true;
    }
    long total;
    try {
      total = readPressureFullTotal();
    } catch (ResourceHandlerException ex) {
      LOG.debug("Could not read memory.pressure", ex);
      return true;
    }
    if (total < 0) {
      return true;
    }
    boolean stalled = lastPressureTotalUs >= 0 && total > lastPressureTotalUs;
    lastPressureTotalUs = total;
    return stalled;
  }

  /**
   * Probe memory.pressure once, at construction. RHEL ships PSI compiled in
   * but switched off, so the file being absent is the expected case on our
   * fleet and not a misconfiguration: report it once, at INFO, and never
   * again.
   */
  private boolean isPressureReadable() {
    try {
      if (readPressureFullTotal() >= 0) {
        return true;
      }
    } catch (ResourceHandlerException ex) {
      LOG.debug("Could not read memory.pressure", ex);
    }
    LOG.info("Kernel memory pressure information unavailable at {} (psi=1 is"
        + " not set): using the memory footprint condition alone.",
        cgroups.getPathForCGroupParam(
            CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_MEMORY_PRESSURE));
    return false;
  }

  private long readPressureFullTotal() throws ResourceHandlerException {
    return parsePressureFullTotal(cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, "", CGROUP_MEMORY_PRESSURE));
  }

  /**
   * Read the full total of a memory.pressure file, whose two lines are
   * <pre>
   * some avg10=0.00 avg60=0.00 avg300=0.00 total=0
   * full avg10=0.00 avg60=0.00 avg300=0.00 total=0
   * </pre>
   * full is the share of time every non-idle task of the cgroup was stalled at
   * once, and total is a monotonic microsecond counter. A delta of total
   * between two polls cannot be missed, whereas an average has lag and can
   * return to zero in between.
   *
   * @param content the contents of memory.pressure
   * @return the full total in microseconds, -1 if it is not there
   */
  @VisibleForTesting
  static long parsePressureFullTotal(String content) {
    for (String line : content.split("\n")) {
      String[] parts = line.trim().split("\\s+");
      if (parts.length < 2 || !"full".equals(parts[0])) {
        continue;
      }
      for (String part : parts) {
        if (part.startsWith("total=")) {
          try {
            return Long.parseLong(part.substring("total=".length()));
          } catch (NumberFormatException ex) {
            return -1;
          }
        }
      }
    }
    return -1;
  }

  /**
   * With cgroup v2 the page cache is not dropped before the node manager is
   * notified, so it has to be excluded explicitly. See
   * {@link CGroupsV2MemoryStat}.
   */
  @Override
  protected long getMemoryUsage(String cGroupId)
      throws ResourceHandlerException {
    return CGroupsV2MemoryStat.readFootprint(
        cgroups, cGroupId, enforceVirtualMemory);
  }

  /**
   * One write to cgroup.kill. Whenever that write fails, every process of
   * the cgroup is signalled individually as on v1.
   */
  @Override
  protected boolean sigKill(Container container) {
    try {
      cgroupKill(container);
      return true;
    } catch (ResourceHandlerException | IOException ex) {
      LOG.warn(String.format("Could not kill container %s through"
              + " cgroup.kill, falling back to signalling every pid.",
          container.getContainerId()), ex);
    }
    return super.sigKill(container);
  }

  @Override
  protected void onContainerKilled(Container container) {
    metrics.containersKilled.incr();
  }

  /**
   * Kill every process of the container with a single write. cgroup.kill kills
   * the whole subtree, so a docker child cgroup goes with it, and unlike the
   * kernel OOM killer it does not increment memory.events' oom_kill, which is
   * what keeps that counter a discriminator of the kernel's own kills.
   *
   * @param container Container to clean up
   * @throws ResourceHandlerException cgroup.procs could not be read
   * @throws IOException cgroup.kill could not be written, which is also the
   *                     case on kernels older than 5.14, where it is absent
   */
  private void cgroupKill(Container container)
      throws ResourceHandlerException, IOException {
    String containerId = container.getContainerId().toString();
    // cgroup.kill has no controller prefix, so it cannot go through
    // updateCGroupParam.
    Path killFile = Paths.get(
        cgroups.getPathForCGroup(
            CGroupsHandler.CGroupController.MEMORY, containerId),
        CGROUP_KILL_FILE);
    Files.write(killFile, "1".getBytes(StandardCharsets.UTF_8));
    LOG.debug("Terminating container {} by writing to {}",
        containerId, killFile);
    for (int attempt = 0; attempt < CGROUP_KILL_WAIT_ATTEMPTS; ++attempt) {
      if (cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
          containerId, CGROUP_PROCS_FILE).isEmpty()) {
        return;
      }
      try {
        Thread.sleep(CGROUP_KILL_WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for processes to disappear");
        return;
      }
    }
    // The signals are delivered, only the reaping is late. Nothing is left to
    // do here but say so: the memory is on its way back either way.
    LOG.warn("Container {} still has processes {} ms after cgroup.kill",
        containerId, CGROUP_KILL_WAIT_ATTEMPTS * CGROUP_KILL_WAIT_INTERVAL_MS);
  }

  /**
   * Kill a container, then give the kernel time to reclaim its pages before
   * the caller decides that another one has to go too. Unlike v1, where the
   * cgroup is frozen while we work, nothing here stops the condition from
   * still reading as out of memory for as long as the reclaim takes.
   */
  @Override
  protected boolean killContainer() {
    boolean containerKilled = super.killContainer();
    if (containerKilled && postKillDelayMs > 0) {
      try {
        Thread.sleep(postKillDelayMs);
      } catch (InterruptedException ex) {
        LOG.debug("Interrupted while waiting after a kill");
      }
    }
    return containerKilled;
  }
}
