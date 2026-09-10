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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_HIGH;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_PRESSURE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_MEMORY_SWAP_MAX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_V2_NO_LIMIT;

/**
 * Elastic memory control on cgroup v2.
 *
 * cgroup v2 has no oom_kill_disable, so the kernel cannot be asked to freeze
 * the cgroup at its limit: memory.max is always a kill. The node manager
 * therefore gets its warning from memory.high, which the kernel sets below
 * memory.max and which only throttles and reclaims, and it kills a container
 * of its choosing before memory.max is reached.
 *
 * A memory.high event is only a wake-up. The kernel raises it before it even
 * attempts reclaim, on a total usage that includes the page cache, so it says
 * nothing about whether the footprint is reclaimable. The condition that
 * actually justifies a kill is the memory footprint of
 * {@link CGroupsV2MemoryStat} standing above memory.high without
 * interruption for the configured hold duration, optionally narrowed by the
 * kernel reporting that memory really did stall.
 */
class CGroupsV2ElasticMemoryController extends CGroupElasticMemoryController {

  /**
   * The line oom_listener_v2() writes to its standard error when the kernel
   * OOM killer got there first. See oom_listener.c.
   */
  private static final Pattern OOM_KILL_PATTERN =
      Pattern.compile("oom_kill increased by (\\d+)");

  /**
   * The floor of the default memory.high margin. Below this the throttling
   * band is too narrow to react in: a handful of containers allocating at
   * once cross it faster than a kill can free anything.
   */
  private static final long DEFAULT_HIGH_MARGIN_FLOOR_BYTES = 512L * 1024 * 1024;

  /** The divisor of the default memory.high margin: 5% of the limit. */
  private static final long DEFAULT_HIGH_MARGIN_DIVISOR = 20;

  private final Clock clock = new MonotonicClock();
  private final long highMarginBytes;
  private final long holdDurationMs;
  private final boolean pressureEnabled;

  /**
   * When the footprint condition started holding, 0 when it does not hold.
   * Guarded by the monitor of this object: the OOM handler and the watchdog
   * of the base class both evaluate the condition.
   */
  private long firstBreachAtMs;

  /** The previous sample of memory.pressure's full total, -1 for none. */
  private long lastPressureTotalUs = -1;

  CGroupsV2ElasticMemoryController(Configuration conf,
                                   Context context,
                                   CGroupsHandler cgroups,
                                   boolean controlPhysicalMemory,
                                   boolean controlVirtualMemory,
                                   long limit,
                                   Runnable oomHandlerOverride)
      throws YarnException {
    super(conf, context, cgroups, controlPhysicalMemory, controlVirtualMemory,
        limit, oomHandlerOverride);
    this.highMarginBytes = computeHighMargin();
    this.holdDurationMs = conf.getLong(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_OOM_HOLD_DURATION_MS);
    this.pressureEnabled = conf.getBoolean(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_PRESSURE_ENABLED)
        && isPressureReadable();
    if (getOOMHandler() instanceof DefaultOOMHandler) {
      ((DefaultOOMHandler) getOOMHandler()).setPostKillDelayMs(conf.getLong(
          NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS,
          DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_POST_KILL_DELAY_MS));
    }
  }

  @Override
  protected String[] oomListenerArgs() {
    return new String[] {"2", yarnCGroupPath};
  }

  /**
   * The distance between memory.high, where the kernel starts throttling, and
   * memory.max, where it kills. Configured in MiB, or 5% of the limit with a
   * floor when the configured value is negative.
   */
  private long computeHighMargin() {
    int configuredMb = conf.getInt(
        NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_CGROUPS_V2_HIGH_MARGIN_MB);
    long margin = configuredMb >= 0
        ? configuredMb * 1024L * 1024L
        : Math.max(limit / DEFAULT_HIGH_MARGIN_DIVISOR,
            DEFAULT_HIGH_MARGIN_FLOOR_BYTES);
    if (margin >= limit) {
      LOG.warn("A memory.high margin of {} bytes does not fit under the {}"
          + " byte limit of {}. Using half of the limit instead.",
          margin, limit, yarnCGroupPath);
      margin = limit / 2;
    }
    return margin;
  }

  /**
   * Set memory.max, the hard limit, and memory.high a margin below it, so
   * that the kernel throttles and reclaims before it kills.
   *
   * memory.oom.group is deliberately left alone: the node manager picks the
   * victim, not the kernel.
   */
  @Override
  protected void setCGroupParameters() throws ResourceHandlerException {
    String max = Long.toString(limit);
    String high = Long.toString(limit - highMarginBytes);
    if (controlVirtualMemory) {
      updateRoot(CGROUP_MEMORY_MAX, max);
      updateRoot(CGROUP_MEMORY_HIGH, high);
      // rss + swap is the enforced dimension, so swap counts against the
      // limit. It has to be set after the physical limit, as in v1.
      updateRoot(CGROUP_MEMORY_SWAP_MAX, max);
    } else {
      try {
        // Physical memory is the enforced dimension: swapping a container out
        // would hide it from the limit we enforce.
        updateRoot(CGROUP_MEMORY_SWAP_MAX, "0");
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap accounting is turned off in the kernel");
      }
      updateRoot(CGROUP_MEMORY_MAX, max);
      updateRoot(CGROUP_MEMORY_HIGH, high);
    }
    LOG.info("Set memory.max={} memory.high={} on {}", max, high,
        yarnCGroupPath);
  }

  /**
   * Reset the root memory cgroup to OS defaults. Each write gets its own try:
   * a limit we failed to lift must not make us skip the others.
   */
  @Override
  protected void resetCGroupParameters() {
    // Lift the throttle before the hard limit, so the cgroup is never left
    // throttling at a value above a limit that is already gone.
    resetRoot(CGROUP_MEMORY_HIGH);
    resetRoot(CGROUP_MEMORY_MAX);
    resetRoot(CGROUP_MEMORY_SWAP_MAX);
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
   */
  @Override
  protected synchronized boolean isUnderOOM() throws ResourceHandlerException {
    long now = clock.getTime();
    if (!isOverHighWatermark()) {
      firstBreachAtMs = 0;
    } else if (firstBreachAtMs == 0) {
      firstBreachAtMs = now;
    }
    boolean held = firstBreachAtMs != 0
        && now - firstBreachAtMs >= holdDurationMs;
    // Sampled on every call, whether it is needed or not, so that the delta is
    // always between two consecutive polls.
    boolean stalling = sampleMemoryStall();
    // The pressure term can only ever narrow the condition. It must never be
    // able to trigger a kill the footprint condition alone would not.
    return held && stalling;
  }

  private boolean isOverHighWatermark() throws ResourceHandlerException {
    long footprint = CGroupsV2MemoryStat.readFootprint(cgroups, "",
        controlVirtualMemory);
    long high = CGroupsV2MemoryStat.parseLimit(cgroups.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, "", CGROUP_MEMORY_HIGH));
    if (footprint > high) {
      LOG.debug("{} uses {} bytes over its memory.high of {}",
          yarnCGroupPath, footprint, high);
      return true;
    }
    return false;
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
   * The listener reports an oom_kill increment when the kernel OOM killer
   * acted inside the YARN cgroup before we could choose a victim. Neither
   * cgroup.kill nor a SIGKILL through container-executor goes through the OOM
   * killer, so this counter only ever counts the kernel's own kills.
   */
  @Override
  protected void onListenerError(String line) {
    Matcher matcher = OOM_KILL_PATTERN.matcher(line);
    if (!matcher.find()) {
      LOG.warn("oom-listener: {}", line);
      return;
    }
    LOG.warn("The kernel OOM killer acted inside {} before the NodeManager"
        + " could choose a victim: {}", yarnCGroupPath, line);
    NodeManagerMetrics metrics =
        context == null ? null : context.getNodeManagerMetrics();
    if (metrics != null) {
      metrics.kernelOomKills(Long.parseLong(matcher.group(1)));
    }
  }

  private void updateRoot(String param, String value)
      throws ResourceHandlerException {
    cgroups.updateCGroupParam(
        CGroupsHandler.CGroupController.MEMORY, "", param, value);
  }

  private void resetRoot(String param) {
    try {
      updateRoot(param, CGROUP_V2_NO_LIMIT);
    } catch (ResourceHandlerException ex) {
      LOG.warn("Error in cleanup of memory." + param, ex);
    }
  }
}
